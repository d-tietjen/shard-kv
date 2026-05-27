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
    ($type:ident, $name:literal, $mutates:expr, aliases: [$($alias:literal),+ $(,)?]) => {
        #[derive(Debug, Clone, Copy)]
        pub(crate) struct $type;

        pub(crate) static COMMAND: $type = $type;

        impl crate::commands::CommandSpec for $type {
            const NAME: &'static str = $name;
            const MUTATES_VALUE: bool = $mutates;

            #[inline(always)]
            fn matches(name: &[u8]) -> bool {
                name.eq_ignore_ascii_case($name.as_bytes())
                    $(|| name.eq_ignore_ascii_case($alias.as_bytes()))+
            }
        }
    };
}
