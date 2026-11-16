use tokio::sync::mpsc;

pub(super) struct KeyboardEvent {
    pub code: KeyCode, 
    pub modifiers: KeyModifiers,
}

pub(super) enum KeyCode {
    Char(char),
    Backspace,
    Enter,
    Esc,
    Other,
}

pub(super) struct KeyModifiers {
    pub shift: bool,
    pub control: bool,
    pub alt: bool,
}

pub(super) struct Keyboard {
    events_rx: Option<mpsc::Receiver<KeyboardEvent>>,
}

impl Keyboard {
    pub(super) fn new() -> anyhow::Result<Self> {
        let (events_tx, events_rx) = mpsc::channel(16);
        crossterm::terminal::enable_raw_mode()?;
        
        std::thread::spawn({
            use crossterm::event;

            move || {
                loop {
                    match event::read() {
                        Ok(event::Event::Key(key)) if key.kind == event::KeyEventKind::Press => {
                            let code = match key.code {
                                event::KeyCode::Char(c) => KeyCode::Char(c),
                                event::KeyCode::Backspace => KeyCode::Backspace,
                                event::KeyCode::Enter => KeyCode::Enter,
                                event::KeyCode::Esc => KeyCode::Esc,
                                _ => KeyCode::Other,
                            };

                            let modifiers = KeyModifiers {
                                shift: key.modifiers.contains(event::KeyModifiers::SHIFT),
                                control: key.modifiers.contains(event::KeyModifiers::CONTROL),
                                alt: key.modifiers.contains(event::KeyModifiers::ALT),
                            };

                            if events_tx.blocking_send(KeyboardEvent{ code, modifiers }).is_err() {
                                return;
                            }
                        }
                        Ok(_) => {}
                        Err(_) => return
                    }
                }
            }
        });

        Ok(Self {
            events_rx: Some(events_rx),
        })
    }

    pub(super) fn take_events(&mut self) -> mpsc::Receiver<KeyboardEvent> {
        self.events_rx
            .take()
            .expect("events_rx already taken")
    }
}
