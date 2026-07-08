select
    item_a,
    item_b,
    pair_type,
    weight
from {{ source('raw', 'cooccur_pairs') }}
