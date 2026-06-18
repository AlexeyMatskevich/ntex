#![allow(clippy::missing_panics_doc)]
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};
use std::{cell::Cell, fmt, io, thread};
use std::{collections::VecDeque, num::NonZeroUsize};

use ntex_polling::{Event, Events, Poller};
use ntex_rt::System;
use ntex_util::{future::Either, time::Millis, time::sleep};

use super::socket::{Connection, Listener, SocketAddr};
use super::{Server, ServerStatus, Token};

const EXIT_TIMEOUT: Duration = Duration::from_millis(100);
const ERR_TIMEOUT: Duration = Duration::from_millis(500);
const ERR_SLEEP_TIMEOUT: Millis = Millis(525);

#[derive(Debug)]
pub enum AcceptorCommand {
    Stop(oneshot::Sender<()>),
    Terminate,
    Pause,
    Resume,
    Timer,
}

impl AcceptorCommand {
    fn label(&self) -> &'static str {
        match self {
            AcceptorCommand::Stop(_) => "Stop",
            AcceptorCommand::Terminate => "Terminate",
            AcceptorCommand::Pause => "Pause",
            AcceptorCommand::Resume => "Resume",
            AcceptorCommand::Timer => "Timer",
        }
    }
}

#[derive(Debug)]
struct ServerSocketInfo {
    addr: SocketAddr,
    token: Token,
    sock: Listener,
    registered: Cell<bool>,
    timeout: Cell<Option<Instant>>,
}

#[derive(Debug, Clone)]
pub struct AcceptNotify {
    poller: Arc<Poller>,
    tx: mpsc::Sender<AcceptorCommand>,
    name: Arc<Mutex<String>>,
    seq: Arc<AtomicU64>,
}

impl AcceptNotify {
    fn new(poller: Arc<Poller>, tx: mpsc::Sender<AcceptorCommand>, name: String) -> Self {
        AcceptNotify {
            poller,
            tx,
            name: Arc::new(Mutex::new(name)),
            seq: Arc::new(AtomicU64::new(0)),
        }
    }

    fn set_name(&self, name: String) {
        *self.name.lock().unwrap_or_else(|err| err.into_inner()) = name;
    }

    fn name(&self) -> String {
        self.name
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .clone()
    }

    pub fn send(&self, cmd: AcceptorCommand) {
        let label = cmd.label();
        let seq = self.seq.fetch_add(1, Ordering::Relaxed) + 1;
        let name = self.name();

        log::trace!("AcceptNotify {name:?} sending {label} seq={seq}");
        match self.tx.send(cmd) {
            Ok(()) => {
                log::trace!("AcceptNotify {name:?} queued {label} seq={seq}");
            }
            Err(err) => {
                log::error!(
                    "AcceptNotify {name:?} failed to queue {label} seq={seq}: {err:?}"
                );
                return;
            }
        }

        match self.poller.notify() {
            Ok(()) => {
                log::trace!("AcceptNotify {name:?} notified poller for {label} seq={seq}");
            }
            Err(err) => {
                log::error!(
                    "AcceptNotify {name:?} failed to notify poller for {label} seq={seq}: {err}"
                );
            }
        }
    }
}

/// Streamin io accept loop
pub struct AcceptLoop {
    name: String,
    testing: bool,
    notify: AcceptNotify,
    inner: Option<(mpsc::Receiver<AcceptorCommand>, Arc<Poller>)>,
    status_handler: Option<Box<dyn FnMut(ServerStatus) + Send>>,
}

impl Default for AcceptLoop {
    fn default() -> Self {
        Self::new()
    }
}

impl AcceptLoop {
    /// Create accept loop
    pub fn new() -> AcceptLoop {
        // Create a poller instance
        let poll = Arc::new(
            Poller::new()
                .map_err(|e| panic!("Cannot create Poller {e}"))
                .unwrap(),
        );

        let (tx, rx) = mpsc::channel();
        let name = "ntex:accept".to_string();
        let notify = AcceptNotify::new(poll.clone(), tx, name.clone());

        AcceptLoop {
            notify,
            name,
            inner: Some((rx, poll)),
            testing: false,
            status_handler: None,
        }
    }

    /// Set server name.
    ///
    /// Name is used for worker thread name
    pub fn name<T: AsRef<str>>(&mut self, name: T) {
        self.name = format!("{}:accept", name.as_ref());
        self.notify.set_name(self.name.clone());
    }

    /// Get notification api for the loop
    pub fn notify(&self) -> AcceptNotify {
        self.notify.clone()
    }

    pub fn set_status_handler<F>(&mut self, f: F)
    where
        F: FnMut(ServerStatus) + Send + 'static,
    {
        self.status_handler = Some(Box::new(f));
    }

    pub fn testing(&mut self) {
        self.testing = true;
    }

    /// Start accept loop
    pub fn start(mut self, socks: Vec<(Token, Listener)>, srv: Server) {
        let (tx, rx_start) = oneshot::channel();
        let (rx, poll) = self
            .inner
            .take()
            .expect("AcceptLoop cannot be used multiple times");

        Accept::start(
            tx,
            rx,
            poll,
            socks,
            srv,
            self.name.clone(),
            self.notify.clone(),
            self.testing,
            self.status_handler.take(),
        );

        let _ = rx_start.recv();
    }
}

impl fmt::Debug for AcceptLoop {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AcceptLoop")
            .field("name", &self.name)
            .field("notify", &self.notify)
            .field("inner", &self.inner)
            .field("status_handler", &self.status_handler.is_some())
            .finish()
    }
}

struct Accept {
    name: String,
    poller: Arc<Poller>,
    rx: mpsc::Receiver<AcceptorCommand>,
    tx: Option<oneshot::Sender<()>>,
    sockets: Vec<ServerSocketInfo>,
    srv: Server,
    notify: AcceptNotify,
    testing: bool,
    backpressure: bool,
    backlog: VecDeque<Connection>,
    status_handler: Option<Box<dyn FnMut(ServerStatus) + Send>>,
}

impl Accept {
    #[allow(clippy::too_many_arguments)]
    fn start(
        tx: oneshot::Sender<()>,
        rx: mpsc::Receiver<AcceptorCommand>,
        poller: Arc<Poller>,
        socks: Vec<(Token, Listener)>,
        srv: Server,
        name: String,
        notify: AcceptNotify,
        testing: bool,
        status_handler: Option<Box<dyn FnMut(ServerStatus) + Send>>,
    ) {
        log::info!("Starting {name:?} accept loop");
        let accept_name = name.clone();

        // start accept thread
        let sys = System::current();
        let _ = thread::Builder::new().name(name).spawn(move || {
            System::set_current(sys);
            Accept::new(
                tx,
                rx,
                poller,
                socks,
                srv,
                accept_name,
                notify,
                testing,
                status_handler,
            )
            .poll();
        });
    }

    #[allow(clippy::too_many_arguments)]
    fn new(
        tx: oneshot::Sender<()>,
        rx: mpsc::Receiver<AcceptorCommand>,
        poller: Arc<Poller>,
        socks: Vec<(Token, Listener)>,
        srv: Server,
        name: String,
        notify: AcceptNotify,
        testing: bool,
        status_handler: Option<Box<dyn FnMut(ServerStatus) + Send>>,
    ) -> Accept {
        let mut sockets = Vec::new();
        for (hnd_token, lst) in socks {
            sockets.push(ServerSocketInfo {
                addr: lst.local_addr(),
                sock: lst,
                token: hnd_token,
                registered: Cell::new(false),
                timeout: Cell::new(None),
            });
        }

        Accept {
            name,
            poller,
            rx,
            sockets,
            notify,
            srv,
            testing,
            status_handler,
            tx: Some(tx),
            backpressure: true,
            backlog: VecDeque::new(),
        }
    }

    fn update_status(&mut self, st: ServerStatus) {
        if let Some(ref mut hnd) = self.status_handler {
            (*hnd)(st);
        }
    }

    fn poll(mut self) {
        // Create storage for events
        let mut events = Events::with_capacity(NonZeroUsize::new(512).unwrap());

        // notify start
        for idx in 0..self.sockets.len() {
            self.add_source(idx);
        }
        if let Some(tx) = self.tx.take() {
            thread::sleep(Duration::from_millis(25));
            let _ = tx.send(());
        }

        loop {
            if let Either::Right(rx) = self.process_cmd() {
                self.stop_loop(rx);
                break;
            }

            events.clear();

            log::trace!(
                "Accept loop {:?} waiting on poller, backpressure={}",
                self.name,
                self.backpressure
            );
            if let Err(e) = self.poller.wait(&mut events, None) {
                assert!(
                    e.kind() == io::ErrorKind::Interrupted,
                    "Cannot wait for events in poller: {e}"
                );
            }
            let event_summary: Vec<_> = events.iter().collect();
            log::trace!(
                "Accept loop {:?} woke from poller wait with {} events: {:?}",
                self.name,
                event_summary.len(),
                event_summary
            );

            for event in event_summary {
                let idx = event.key;
                if let Some(info) = self.sockets.get(idx) {
                    if event.is_err().unwrap_or(false) || event.is_interrupt() {
                        log::warn!(
                            "Accept loop {:?} received socket poll error event on {}: {:?}; state: {}",
                            self.name,
                            info.addr,
                            event,
                            info.sock.debug_state()
                        );
                    }
                    if !info.registered.get() {
                        continue;
                    }
                    let readd = self.accept(idx);
                    if readd {
                        self.add_source(idx);
                    }
                } else {
                    log::warn!(
                        "Accept loop {:?} received event for unknown socket key {}: {:?}",
                        self.name,
                        idx,
                        event
                    );
                }
            }

            match self.process_cmd() {
                Either::Left(()) => (),
                Either::Right(rx) => {
                    self.stop_loop(rx);
                    break;
                }
            }
        }
    }

    fn stop_loop(&mut self, rx: Option<oneshot::Sender<()>>) {
        for info in self.sockets.drain(..) {
            info.sock.remove_source();
        }
        log::info!("Accept loop {:?} has been stopped", self.name);

        if let Some(rx) = rx {
            if !self.testing {
                thread::sleep(EXIT_TIMEOUT);
            }
            let _ = rx.send(());
        }
    }

    fn add_source(&self, idx: usize) {
        let info = &self.sockets[idx];

        loop {
            // try to register poller source
            let result = if info.registered.get() {
                self.poller.modify(&info.sock, Event::readable(idx))
            } else {
                unsafe { self.poller.add(&info.sock, Event::readable(idx)) }
            };
            if let Err(err) = result {
                if err.kind() == io::ErrorKind::WouldBlock {
                    continue;
                }
                log::error!("Cannot register socket listener: {err}");

                // sleep after error
                info.timeout.set(Some(Instant::now() + ERR_TIMEOUT));

                let notify = self.notify.clone();
                System::current().handle().spawn(async move {
                    sleep(ERR_SLEEP_TIMEOUT).await;
                    notify.send(AcceptorCommand::Timer);
                });
            } else {
                info.registered.set(true);
            }

            break;
        }
    }

    fn remove_source(&self, key: usize) {
        let info = &self.sockets[key];

        let result = if info.registered.get() {
            self.poller.modify(&info.sock, Event::none(key))
        } else {
            return;
        };

        // stop listening for incoming connections
        if let Err(err) = result {
            log::error!("Cannot stop socket listener for {} err: {}", info.addr, err);
        }
    }

    fn process_timer(&mut self) {
        let now = Instant::now();
        for key in 0..self.sockets.len() {
            let info = &mut self.sockets[key];
            if let Some(inst) = info.timeout.get()
                && now > inst
                && !self.backpressure
            {
                log::info!("Resuming socket listener on {} after timeout", info.addr);
                info.timeout.take();
                self.add_source(key);
            }
        }
    }

    fn process_cmd(&mut self) -> Either<(), Option<oneshot::Sender<()>>> {
        loop {
            match self.rx.try_recv() {
                Ok(cmd) => match cmd {
                    AcceptorCommand::Stop(rx) => {
                        if !self.backpressure {
                            log::info!("Stopping {:?} accept loop", self.name);
                            self.backpressure(true);
                        }
                        break Either::Right(Some(rx));
                    }
                    AcceptorCommand::Terminate => {
                        log::info!("Stopping {:?} accept loop", self.name);
                        self.backpressure(true);
                        break Either::Right(None);
                    }
                    AcceptorCommand::Pause => {
                        log::trace!(
                            "Accept loop {:?} received Pause, backpressure={}",
                            self.name,
                            self.backpressure
                        );
                        if !self.backpressure {
                            log::info!("Pausing {:?} accept loop", self.name);
                            self.backpressure(true);
                        }
                    }
                    AcceptorCommand::Resume => {
                        log::trace!(
                            "Accept loop {:?} received Resume, backpressure={}",
                            self.name,
                            self.backpressure
                        );
                        if self.backpressure {
                            log::info!("Resuming {:?} accept loop", self.name);
                            self.backpressure(false);
                        }
                    }
                    AcceptorCommand::Timer => {
                        self.process_timer();
                    }
                },
                Err(err) => {
                    break match err {
                        mpsc::TryRecvError::Empty => Either::Left(()),
                        mpsc::TryRecvError::Disconnected => {
                            log::error!("Dropping accept loop");
                            self.backpressure(true);
                            Either::Right(None)
                        }
                    };
                }
            }
        }
    }

    fn backpressure(&mut self, on: bool) {
        log::trace!(
            "Accept loop {:?} updating status to {:?}",
            self.name,
            if on {
                ServerStatus::NotReady
            } else {
                ServerStatus::Ready
            }
        );
        self.update_status(if on {
            ServerStatus::NotReady
        } else {
            ServerStatus::Ready
        });

        if self.backpressure && !on {
            // handle backlog
            while let Some(msg) = self.backlog.pop_front() {
                if let Err(msg) = self.srv.process(msg) {
                    log::trace!("Server is unavailable");
                    self.backlog.push_front(msg);
                    return;
                }
            }

            // re-enable acceptors
            self.backpressure = false;
            for (key, info) in self.sockets.iter().enumerate() {
                if info.timeout.get().is_none() {
                    // socket with timeout will re-register itself after timeout
                    log::info!(
                        "Resuming socket listener on {} after back-pressure",
                        info.addr
                    );
                    self.add_source(key);
                }
            }
        } else if !self.backpressure && on {
            self.backpressure = true;
            for key in 0..self.sockets.len() {
                // disable err timeout
                let info = &mut self.sockets[key];
                if info.timeout.take().is_none() {
                    log::info!("Stopping socket listener on {}", info.addr);
                    self.remove_source(key);
                }
            }
        }
    }

    fn accept(&mut self, token: usize) -> bool {
        let mut connection_errors = 0usize;
        loop {
            if let Some(info) = self.sockets.get_mut(token) {
                match info.sock.accept() {
                    Ok(Some(io)) => {
                        log::trace!(
                            "Accept loop {:?} accepted connection on {}",
                            self.name,
                            info.addr
                        );
                        let msg = Connection {
                            io,
                            token: info.token,
                        };
                        if let Err(msg) = self.srv.process(msg) {
                            log::trace!("Server is unavailable");
                            self.backlog.push_back(msg);
                            self.backpressure(true);
                            return false;
                        }
                    }
                    Ok(None) => {
                        log::trace!(
                            "Accept loop {:?} accept returned None for {}",
                            self.name,
                            info.addr
                        );
                        return true;
                    }
                    Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                        log::trace!(
                            "Accept loop {:?} accept would block on {}",
                            self.name,
                            info.addr
                        );
                        return true;
                    }
                    Err(ref e) if connection_error(e) => {
                        connection_errors += 1;
                        if connection_errors <= 8 || connection_errors.is_power_of_two() {
                            log::warn!(
                                "Accept loop {:?} ignoring connection accept error #{} on {}: {e}",
                                self.name,
                                connection_errors,
                                info.addr
                            );
                        }
                    }
                    Err(e) => {
                        log::error!(
                            "Error accepting socket: {e}; state: {}",
                            info.sock.debug_state()
                        );

                        // sleep after error
                        info.timeout.set(Some(Instant::now() + ERR_TIMEOUT));

                        let notify = self.notify.clone();
                        System::current().handle().spawn(async move {
                            sleep(ERR_SLEEP_TIMEOUT).await;
                            notify.send(AcceptorCommand::Timer);
                        });
                        return false;
                    }
                }
            }
        }
    }
}

/// This function defines errors that are per-connection. Which basically
/// means that if we get this error from `accept()` system call it means
/// next connection might be ready to be accepted.
///
/// All other errors will incur a timeout before next `accept()` is performed.
/// The timeout is useful to handle resource exhaustion errors like ENFILE
/// and EMFILE. Otherwise, could enter into tight loop.
fn connection_error(e: &io::Error) -> bool {
    e.kind() == io::ErrorKind::ConnectionRefused
        || e.kind() == io::ErrorKind::ConnectionAborted
        || e.kind() == io::ErrorKind::ConnectionReset
}
