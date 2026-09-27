# Research

This folder holds notebooks for exploratory research work. Nothing here is scheduled, and nothing here writes to production tables 
or files. Research notebooks may read production data, but their outputs go to a separate research location.

## How the branches work

This folder lives only on the `research` branch. The `research` branch never merges into `main`, so experiments and unfinished work stay out of production code.

To keep research up to date with production, merge `main` into `research` from time to time. On GitHub, open a pull request with `research` as the base and `main` as the compare branch, merge it, then pull in the research Git folder. Always merge in this direction, never the reverse.

## Moving work into production

When something here is ready for production, do not merge this branch. Instead, from the production Git folder, create a new feature branch off `main`, copy in only the piece you need, clean it up, test it, and merge it into `main` as usual.