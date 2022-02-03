# Meshanina --- specialized, WORM, content-addressed database

Meshanina is a rather strange key-value database, with three assumptions:

- Once written, a key-value mapping will never be deleted
- Some function `H` maps every value to every key: `H(v) = k`. That is, the same key will never be rebound to a different value.

Meshanina is designed for use as a _content-addressed_ datastore, where keys are typically hashes of values and deletion is ill-defined.

## Database layout

**NOTE: THIS IS WRONG AND OUTDATED. Will fix soon.**

A Meshanina database maps 256-bit integers to arbitrary byteslices. At its heart, it is simply an open-addressed hashtable mapping 256-bit keys to "records".

Database files are fixed-size. When they fill up, a new one twice as big is created and all records are moved across.

## Record

Each record takes up exactly 1024 bytes, and looks like this:

- 4-bytes crc32 checksum of record
- 32-byte key
- 4-byte value length
- value
- zero padding to next 1024-byte boundary

This, naturally, does not support values that are bigger than 512-4-32-4=**984 bytes**. Bigger values are supported through a separate mechanism.

## Lookup

We use linear probing: first we modulo the key with the number of records to find a "record number", then we scan until we find the correct one or hit an empty slot. Slots with non-validating checksums are treated as "empty", nonallocated space.

## Insertion

We probe for an "empty" space to insert the key. As a special case, if we find a record with identical key, we simply abort insertion.

## Durability

Assuming no "blast radius" (edits to one record cannot corrupt other records), we have the following important property: **once a key is bound to a value, and its records are safely on disk, the binding can never be corrupted no matter how wrongly subsequent writes go**.

Achieving this property is the main reason why Meshanina chooses to use a naive open-addressed hashtable. It's much harder if we use b-trees or other linked data structures, especially without any kind of journaling or crash-recovery mechanism.
