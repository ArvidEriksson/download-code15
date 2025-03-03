Run the following in succession. Will duplicate files when creating train/val/test splits.

The following will download, extract, and create splits in the current working directory

(extract_code15 will extract ALL zip files in target directory and then remove them)

```bash
python3 download_code15.py
python3 extract_code15.py
python3 generateh5_code15.py
```
