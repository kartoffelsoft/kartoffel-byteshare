uniffi::setup_scaffolding!();

#[uniffi::export]
pub fn hello(name: String) -> String {
    format!("Hello, {}!", name)
}
