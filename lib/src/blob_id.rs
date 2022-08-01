use crate::crypto::{Digest, Hashable};
use rand::{
    distributions::{Distribution, Standard},
    Rng,
};

define_byte_array_wrapper! {
    /// BlobId is used to identify a blob in a directory
    pub(crate) struct BlobId([u8; 32]);
}

derive_sqlx_traits_for_byte_array_wrapper!(BlobId);

impl BlobId {
    pub(crate) const ROOT: Self = Self([0; Self::SIZE]);
}

// Never generates `ROOT`
impl Distribution<BlobId> for Standard {
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> BlobId {
        loop {
            let sample = BlobId(self.sample(rng));
            if sample != BlobId::ROOT {
                return sample;
            }
        }
    }
}

impl Hashable for BlobId {
    fn update_hash<S: Digest>(&self, state: &mut S) {
        self.0.update_hash(state)
    }
}
