from typing import List
from ..core.domain import RawMetadata
from ...config.settings import CGIAR_CENTERS

class CGIARFilter:
    """Filters records to ensure they are CGIAR-affiliated."""

    def __init__(self):
        self._centers_normalization = [c.lower() for c in CGIAR_CENTERS]

    def is_cgiar_affiliated(self, record: RawMetadata) -> bool:
        """
        Determines if a record is CGIAR affiliated based on metadata signals.
        1. Checks explicit 'affiliation' field.
        2. Checks if 'CGIAR' is mentioned in verified contexts.
        """
        # 1. Check Affiliation Field
        if record.affiliation:
            aff_lower = record.affiliation.lower()
            if any(center in aff_lower for center in self._centers_normalization):
                return True

        repo = record.repository_source.lower()
        if "cgiar" in repo or "cgspace" in repo:
            # If the repository is explicitly a CGIAR repo (like CGSpace), 
            # we generally trust it.
            return True

        # 4. Check Authors (Fallback)
        # If affiliation field is missing, check if center name appears in author strings
        # (Some connectors might put affiliation in author name like "Name (Center)")
        if record.authors:
            for author in record.authors:
                auth_lower = author.lower()
                if any(center in auth_lower for center in self._centers_normalization):
                    return True

        return False

    def filter_batch(self, records: List[RawMetadata]) -> List[RawMetadata]:
        return [r for r in records if self.is_cgiar_affiliated(r)]
