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

- Checksum of record
- Key
- Value length
- Value
- Padding to next 512-byte boundary

If the value length is greater than 512, this means that the record "overflows" into the one with the numerically "next" key.

## Lookup

We use linear probing: first we modulo the key with the number of records to find a "record number", then we scan until we find the correct one or hit an empty slot.
