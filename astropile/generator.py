import datasets
from astropy.table import Table
from astropy.coordinates import SkyCoord
from astropy import units as u

def build_cross_matched_dataset(readers,
                                keys):
    """ Creates a new Hugging Face dataset given description provided by the user.
    """

    def generator_fn():
        for i, examples in enumerate(zip([r.get_examples(k) for (r,k) in zip(readers, keys)])):
            example = {}
            for ex in examples:
                example.update(ex)
            yield i, example

    return datasets.Dataset.from_generator(generator_fn)

