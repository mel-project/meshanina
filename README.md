# Meshanina --- specialized, WORM, content-addressed database

Meshanina is a rather strange key-value database, with three assumptions:

- Once written, a key-value mapping will never be deleted
- Some function `H` maps every value to every key: `H(v) = k`. That is, the same key will never be rebound to a different value.

Meshanina is designed for use as a _content-addressed_ datastore, where keys are typically hashes of values and deletion is ill-defined. It is a purely log-structured, content-addressed database file where we interleave data blocks with 64-ary HAMT nodes. When we insert keys, we just insert gobs of data, then when we flush we make sure metadata is pushed out too.

In-memory, we keep track of an Arc-linked bunch of new nodes before they get flushed out. Everything is managed in a "purely functional" way.

## On-disk format

- 4 KiB: reserved region
  - starting with 10 bytes: `meshanina2`
  - then 16 bytes more of a random, unique, database-specific 128-bit divider
- indefinite number of **records**:
  - (possibly padding to some nice boundary)
  - 16 bytes: magic divider stored in the reserved region
  - 8 bytes: SipHash 1-3 checksum of the record contents
  - 4 bytes: what kind of record, little endian
    - 0x00000000: data
    - 0x00000001: HAMT _interior_ node
    - 0x00000002: HAMT _root_ node
  - 4 bytes: length of the record
  - n bytes: the content of the record
    - for HAMT nodes, this is:
      - 8 bytes: 64-bit little-endian bitmap
      - n\*8 bytes: 64-bit pointers to absolute offsets
    - for data nodes, this is:
      - 32 bytes: key
      - n bytes: value (LZ4 compressed with prepended size)

## Recovery

On DB open, there is a recovery mechanism. We search backwards, from the end of the file, for instances of the magic divider, then try to decode a record at each instance. When we find the first _validly encoded HAMT root_, we stop. We then use this root as the starting point for the database.

Assuming that there are no "gaps" in correctly written blocks --- that is, if there's a record that's correctly written, every record before it must be so too --- this defends against arbitrary crashes and power interruptions. Essentially all Unix filesystems do guarantee that interrupted file appends cannot disturb existing data in the file.

## Lookup and insertion

Starting from the latest HAMT root node, do the usual HAMT lookup/insertion, using the 256-bit key value 6 bits at a time. The implementation currently only uses the first 128 bits of the key for indexing purposes.

The library automatically compresses values using LZ4 compression with size prepended before storing them in the database, and decompresses them when retrieving.