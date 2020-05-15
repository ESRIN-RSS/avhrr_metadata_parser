# avhrr_metadata_parser
The avhrr_metadata_parser is a script that searches for metadata of a given avhrr product (or list of products) in a set of CSV files. The attained data is parsed, formated and printed to stdout. The script also has the capability to properly organize and zip the input data into a new folder with the structure "datasetname/year/month/day/product.tgz".

This README is intended to provide short and quick information on the project. 

## Usage

Execute the avhrr_metadata_parser.py script with *-h* flag for usage help.
```
$ python avhrr_metadata_parser.py -h
usage: avhrr_metadata_parser.py [-h] --output dst_dir
                               [--avhrr_file avhrrproduct_file_or_dir] [--avhrr_list avhrrproduct_list_of_files_or_dirs]
                               [--noaa_mtd metadata_csvs]
                               [-O] [-r] 

avhrr_metadata_parser is a parser intended to print to stdout, properly formated metadata for avhrr products.

optional arguments:
  -h, --help            show this help message and exit
  --noaa_mtd NOAA_MTD   The zipped NOAA sats metadata file remote location
                        (default: ftp://eogrid.esrin.esa.int/Catalogue/Noaa_catalogue_1_1.tgz)
  --output OUTPUT       Output folder for required metadata files (default: None)
  --avhrr_list AVHRR_LIST
                        The list of files to process (default: None)
  --avhrr_file AVHRR_FILE
                        The avhrr file path to process (default: None)
  --ds DS               The dataset name for the avhrr file (default: None)
  -O                    Organize the output folders of the avhrr file
                        (default: False)
  -d                    Read directly from metadata file, instead of csv files. This is needed in order to run .dat 
                        files because those are not well identified in the csv tables.
  -r                    Remove avhrr dir after zipping (default: False)
```

## License

GNU GPLv3
