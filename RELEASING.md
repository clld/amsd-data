
# Releasing AMSD data

## Recreating the CLDF dataset

1. Check the data in `org_data/records.tsv`:
   ```shell
   amsd to_csv --dry-run
   ```
2. Copy missing media files from dropbox to mediafiles/upload:
   ```shell
   amsd copy_media ../images
   ```
3. Upload missing media files to CDSTAR:
   ```shell
   amsd upload_media mediafiles/upload
   ```
4. Split up `org_data/records.tsv` into separate CSV files in `raw`
   ```shell
   amsd to_csv
   ```
5. Recreate CLDF data
   ```shell
   cldfbench makecldf cldfbench_amsd.py --glottolog-version v5.3 --with-cldfreadme --with-zenodo
   ```
6. Validate
   ```shell
   cldf validate cldf
   ```
