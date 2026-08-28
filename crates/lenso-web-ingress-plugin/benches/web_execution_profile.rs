#[allow(dead_code)]
mod benchmark {
    include!("http_ingress.rs");
}

fn main() {
    benchmark::main_profile();
}
