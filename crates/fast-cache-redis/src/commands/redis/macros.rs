macro_rules! define_redis_command {
    ($type:ident, $name:literal, $mutates:expr) => {
        #[derive(Debug, Clone, Copy)]
        pub(crate) struct $type;

        pub(crate) static COMMAND: $type = $type;

        impl crate::commands::CommandSpec for $type {
            const NAME: &'static str = $name;
            const MUTATES_VALUE: bool = $mutates;
        }
    };
}
