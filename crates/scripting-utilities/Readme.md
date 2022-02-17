# Scripting Utilities #

Small helper crates for writing scripty code, such as found in tools.
Contains code that's _just_ complicated or irritating enough that it's worth
deduplicating instead of copy/pasting, but still simple enough to be appropriate
for scripty code.

We care about compile times for this code, so in general try to keep the crates
small, simple, and easy to understand. In accordance with this, this dir
contains a bunch of micro crates instead of one medium utility crate in hopes
that this will keep them more focused and prevent them from metastasizing, and
take advantage of compiler parallelism.