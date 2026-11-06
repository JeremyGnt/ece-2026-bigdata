# ZIP Code Reference Data

Source: Ready APIs, curated-us-zips
https://github.com/ReadyAPIs-com/curated-us-zips

License: CC BY 4.0.

ZIP data curated by Ready APIs from US Census Bureau,
USPS and SimpleMaps sources.

The seed zip_state_reference.csv contains valid
ZIP code / state combinations.

The states.csv seed provides state codes used
for referential integrity testing.

Military addresses (AA, AE, AP) are excluded
from geographic ZIP validation because they
are not included in the source dataset.

The validation is configured as a warning:
generated Bronze addresses may contain invalid
state/ZIP combinations.

For production, reference data should be refreshed,
validated and versioned when postal assignments change.
