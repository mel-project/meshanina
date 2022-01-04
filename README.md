# Meshanina --- specialized, WORM, content-addressed database

Meshanina is a rather strange key-value database, with three assumptions:

- Once written, a key-value mapping will never be deleted
- Some function `H` maps every value to every key: `H(v) = k`. That is, the same key will never be rebound to a different value.

Meshanina is designed for use as a _content-addressed_ datastore, where keys are typically hashes of values and deletion is ill-defined.

## Database layout

A Meshanina database maps 256-bit integers to arbitrary byteslices. At its heart, it is simply an open-addressed hashtable mapping 256-bit keys to "records".

Database files are fixed-size. When they fill up, a new one twice as big is created and all records are moved across.

## Record

Each record takes up exactly 512 bytes, and looks like this:

- 4-bytes crc32 checksum of record
- 32-byte key
- 4-byte value length
- value
- zero padding to next 512-byte boundary

This, naturally, does not support values that are bigger than 512-4-32-4=**472 bytes**. Bigger values are supported through a separate mechanism, detailed below.

## Lookup

We use linear probing: first we modulo the key with the number of records to find a "record number", then we scan until we find the correct one or hit an empty slot. Slots with non-validating checksums are treated as "empty", nonallocated space.

## Insertion

We probe for an "empty" space to insert the key. As a special case, if we find a record with identical key, we always overwrite even though the record won't be empty. This is to make the second attempt succeed in writing a key-value pair if the first attempt somehow failed. This is important especially with bigger values, detailed below --- without this overwriting behavior, big values cannot be atomically and idempotently written.

## Bigger values

Bigger values utilize a per-database _hidden_ one-way hash function, `Q`, as well as a per-database reversible blinding function `X` (for example, `Q` can be blake3 keyed hashing with some secret `k`, while `X` can be chacha8 stream-cipher encryption with that same secret key).

More specifically, when the 4-byte value length is greater than 472, then the "value" field in the record is actually empty. Instead, keys `k_1 = Q(key)`, `k_2 = Q(Q(key))`, etc map to the individual chunks of the value, blinded through `X`.

The purpose of the blinding and hidden hashing is make the lower-level key-value relationship one-to-one, assuming the hash function and blinding function are strong.

If there's any missing chunk, then the entire key-value pair is deemed absent.
