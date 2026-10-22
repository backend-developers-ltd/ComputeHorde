`HTTPOutputVolume` mount paths must be `/output/` followed by a canonical relative path; paths such as `/output//x`, `/output/../x` or `/output/x/` now raise `ValueError` when the job is created.
