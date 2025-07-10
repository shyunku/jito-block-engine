pub mod auth;
pub mod block_engine;
pub mod block;
pub mod bundle;
pub mod packet;
pub mod relayer;
pub mod searcher;
pub mod shared;
pub mod shredstream;

pub fn add(left: u64, right: u64) -> u64 {
    left + right
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
