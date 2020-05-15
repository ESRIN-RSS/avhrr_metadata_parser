import os
import subprocess
import shutil
import argparse
import csv
import re
import datetime
import logging
import tarfile
import zipfile


def setup_cmd_args():
    """Setup command line arguments."""
    parser = argparse.ArgumentParser(description="AVHRR metadata parser for nrtservice to be used for extracting metadata for the AVHRR products.", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    # parser.add_argument("src_dir", help="The root directory containing data to check")
    parser.add_argument("--noaa_mtd", help="The zipped NOAA sats metadata file remote location", default = "ftp://eogrid.esrin.esa.int/Catalogue/Noaa_catalogue_1_1.tgz")
    parser.add_argument("--output", help="Output folder for required metadata files")
    parser.add_argument("--avhrr_list", help="The list of files to process")
    parser.add_argument("--avhrr_file", help="The avhrr file path to process")
    parser.add_argument("--ds", help="The dataset name for the avhrr file")
    parser.add_argument("-O", action='store_true', help="Organize the output folders of the avhrr file")
    parser.add_argument("-f", action='store_true', help="Separate the products without footprint")
    parser.add_argument("-r", action='store_true', help="Remove avhrr dir after zipping")
    parser.add_argument("-d", action='store_true', help="Read directly from metadata file, instead of csv files.")
    parser.add_argument("-l", action='store_true', help="Export csv list of processed/non-processed products")
    return parser.parse_args()


def prepare_datafiles(NOAA_sat_mtd, TMPDIR):
    NOAA_sat_mtd_dir=os.path.join(TMPDIR,"NOAA_sat_mtd")
    NOAA_sat_mtd_zip=os.path.join(TMPDIR,os.path.basename(NOAA_sat_mtd))

    curl_string="curl \""+NOAA_sat_mtd+"\" -o \""+NOAA_sat_mtd_zip+"\""
    try:
        readout1 = subprocess.Popen(curl_string, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=1).communicate()[0]
    except:
        print("Some problem occurred while downloading the file")
    shutil.rmtree(NOAA_sat_mtd_dir, ignore_errors=True)
    os.mkdir(NOAA_sat_mtd_dir)
    try:
        tar_string="tar -xvzf "+NOAA_sat_mtd_zip+" -C "+NOAA_sat_mtd_dir
        readout2 = subprocess.Popen(tar_string, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=1).communicate()[0]
    except:
        print("Some problem occurred while unzipping the file")
    return NOAA_sat_mtd_dir


def find_right_csv(string_to_find,csv_files):
    right_csv=None
    for f in csv_files:
        if string_to_find in open(f).read():
            right_csv=f
    return right_csv


def get_right_line(csvfile, srchstring):
    right_line=None
    reader = csv.reader(open(csvfile, 'r'))
    for data in reader:
        #list index start from 0, thus 2938 is in data[1]
        if any(srchstring in s for s in data):
            right_line=data
    return right_line


def get_size(path):
    total_size = 0
    ext = os.path.splitext(path)[-1]
    if ext in zipped:
        total_size = os.path.getsize(path)
    else:
        for dirpath, dirnames, filenames in os.walk(path):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                total_size += os.path.getsize(fp)
    return total_size


def parse_time(date, time):
    datetime_object = datetime.datetime.strptime(str(date)+str(time), '%y%m%d%H%M%S')
    return datetime_object.strftime('%Y-%m-%dT%H:%M:%S.000Z'), datetime_object


def parse_footprint(corners):
    corners = corners.split()
    try:
        corners = [float(i) for i in corners]
        corners = [str(i) for i in corners]
        footprint = "POLYGON(("+corners[1]+" "+corners[0]+","+corners[3]+" "+corners[2]+","+corners[5]+" "+corners[4]+","+corners[7]+" "+corners[6]+","+corners[1]+" "+corners[0]+"))"
        okfootprint = True
    except:
        okfootprint = False
        # footprint = "POLYGON((0 0, 0 0, 0 0, 0 0, 0 0))"
        footprint = "POLYGON((-180 -90,-180 90,180 90,180 -90,-180 -90))"
    return footprint, okfootprint


def get_dataset(ds, level, footprint):
    if ds == None:
        if footprint:
            ds = 'NOAA_AVHRR_'+level.replace('LEVEL','L').replace(' ','')
        else:
            ds = 'NOAA_AVHRR_' + level.replace('LEVEL', 'L').replace(' ', '') + '_NOFP'
    return ds


def locate(file, root):
    '''Locate all files matching supplied filename pattern in and below
    supplied root directory.'''
    avhrr_file_path=None
    for path, dirs, files in os.walk(os.path.abspath(root)):
        if file in path:
            avhrr_file_path=path
            break
    return avhrr_file_path


def compose_output(img, img_path, theline, ds, footprint):
    ext = os.path.splitext(img_path)[-1]
    if ext in zipped:
        plevel = get_level_in_zipped(img_path, theline)
    else:
        plevel = get_level(os.path.join(img_path, img), theline)
    ds = get_dataset(ds, plevel, footprint)
    acq_station = ''
    if theline[5] != '': acq_station = "acquisition_station=" + theline[5]
    finalstrg = "product=" + img + "\n" \
                "dataset=" + ds + "\n" \
                + re.sub(' +', '_', acq_station) + "\n" \
                "start_orbit_number=" + theline[6].replace("?????","0") + "\n" \
                "size=" + str(get_size(img_path)) + "\n" \
                "start_time=" + str(parse_time(theline[1], theline[2])[0]) + "\n" \
                "stop_time=" + str(parse_time(theline[1], theline[3])[0]) + "\n" \
                "footprint='" + parse_footprint(theline[8])[0] + "'"
    return finalstrg


def folder_structure(outdir, level, date, ds, footprint):
    ds = get_dataset(ds, level, footprint)
    mkpath = outdir
    month = "{0:0=2d}".format(date.month)
    day = "{0:0=2d}".format(date.day)
    for dir in ds, date.year, month, day:
        dir = os.path.join(mkpath,str(dir))
        if not os.path.exists(dir):
            os.mkdir(dir)
        mkpath = os.path.join(mkpath, dir)
    return mkpath


def get_level(file, metadata):
    level = metadata[7]
    if level == '':
        for sf in searchfiles:
            sf = os.path.join(file, sf)
            if os.path.exists(sf):
                # mtdtext = open(sf).read()
                with open(sf, encoding="utf8", errors='ignore') as f:
                    mtdtext = f.read()
                for p in level_patterns:
                    level = re.search(p, mtdtext)
                    if level:
                        level = level.group(0)
                        break
    elif not level[1:] == "L":
        level = "L"+level
    if level == None: level = 'Not available'
    return level


def get_level_in_zipped(ofile, metadata):
    level = metadata[7]
    ext = os.path.splitext(ofile)[-1]
    if ext == ".zip":
        archive = zipfile.ZipFile(ofile, 'r')
        filelist = archive.namelist()
    else:
        tar = tarfile.open(ofile, "r:gz")
        filelist = tar.getmembers()
    if level == '':
        for sf in searchfiles:
            #sf = os.path.join(file, sf)
            if sf in filelist:
                if ext == ".zip":
                    mtdtext = archive.read(sf)
                else:
                    mtdtext = tar.extractfile(sf).read()
                # with open(sf, encoding="utf8", errors='ignore') as f:
                #     mtdtext = f.read()
                for p in level_patterns:
                    level = re.search(p, mtdtext)
                    if level:
                        level = level.group(0)
                        break
    elif not level[1:] == "L":
        level = "L"+level
    if level == None: level = 'Not available'
    return level


def organize(theline,output,full_img_path, ds, footprint):
    timestr, timeobj = parse_time(theline[1], theline[2])
    nf = folder_structure(output, get_level(full_img_path, theline), timeobj, ds, footprint)
    img = os.path.basename(full_img_path)
    img_newpath = os.path.join(nf, img)
    zippedimg = ''
    if not os.path.exists(img_newpath):
        ext = os.path.splitext(full_img_path)[-1]
        if ext in zipped:
            zippedimg = img_newpath
            os.chdir(os.path.dirname(full_img_path))
            shutil.copyfile(full_img_path, img_newpath)
        else:
            zippedimg = img_newpath + ".tgz"
            os.chdir(full_img_path)
            make_tarfile(zippedimg, full_img_path)
        #
        # shutil.copytree(full_img_path, img_newpath)  ##Uncomment if you want to copy also the original img folder into the new destination
        # tar_string = "tar czf " + zippedimg + " *"
        # readout2 = subprocess.Popen(tar_string, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        #                             bufsize=1).communicate()[0]
    return zippedimg


def handle_zipped_input(src):
    ext = os.path.splitext(src)[-1]
    dst_dir = os.path.splitext(src)[0]
    if ext in zipped:
        unzipped = True
        src_dir = src
        # src_dir = os.path.splitext(src)[0]
        # if ext==".zip":
        #     with zipfile.ZipFile(src, 'r') as tar:
        #         tar.extractall(path=dst_dir)
        # else:
        #     tar = tarfile.open(src)
        #     tar.extractall(path=dst_dir)
        #     tar.close()
    else:
        unzipped = False
        src_dir = src
        dst_dir = src
    if os.path.exists(src):
        img = os.path.basename(src_dir)
        return img, src_dir, dst_dir, unzipped


def make_tarfile(output_filename, source_dir):
    with tarfile.open(output_filename, "w:gz") as tar:
        tar.add(source_dir, arcname="") #os.path.basename(source_dir))


def get_right_img_dir(strg):
    oexts = [".l1a", "IMAGE", ".dat"] #, "LEADER"]
    fstring = ''
    if strg[-4:] in zipped:
        fstring = strg
    else:
        if os.path.isfile(strg):
            for filename in os.listdir(os.path.dirname(strg)):
                for g in oexts:
                    if filename.find(g)>=0:
                        fstring = os.path.dirname(strg)
                        break
        elif os.path.isdir(strg) and strg[-5:] == ".SHRK":
            for folder, subfolders, files in os.walk(strg):
                if folder != strg:
                    for filename in files:
                        for g in oexts:
                            if filename.find(g) >= 0:
                                fstring = strg
                                break
        elif os.path.isdir(strg):
            for filename in os.listdir(strg):
                for g in oexts:
                    if filename.find(g)>=0:
                        fstring = strg
                        break
    return fstring


def list_products(records_csv, product, processed_successfully, footprint, dest_dir, proc_level):
    if not records_csv == None:
        line = product + "," + str(processed_successfully) + "," + str(footprint) + "," + str(dest_dir) + "," + str(proc_level)
        f = open(records_csv, 'a+')
        f.write(line+'\n')
        f.close()


def read_ief(img, img_path, ds):
    ext = os.path.splitext(img_path)[-1]
    if ext in zipped:
        tar = tarfile.open(img_path)
        m = tar.extractfile(img + ".ief")
        raw_contents = str(m.read())
        tar.close()
    else:
        ief_file = os.path.join(img_path, img + ".ief")
        m = open(ief_file, "r")
        raw_contents = m.read()
    contents = raw_contents.replace("-", " -")
    raw_footprint = f"{contents.split()[15]} {contents.split()[16]} {contents.split()[17]} {contents.split()[18]} {contents.split()[19]} {contents.split()[20]} {contents.split()[21]} {contents.split()[22]}"
    acq_station = "acquisition_station=" + contents.split()[9][:3]
    finalstrg = "product=" + img + "\n" \
                "dataset=" + ds + "\n" \
                + re.sub(' +', '_', acq_station) + "\n" \
                "start_orbit_number=" + contents.split()[9][6:11].replace("?????", "0") + "\n" \
                "size=" + str(get_size(img_path)) + "\n" \
                "start_time=" + str(parse_time(contents.split()[6], contents.split()[7])[0]) + "\n" \
                "stop_time=" + str(parse_time(contents.split()[6], contents.split()[8])[0]) + "\n" \
                "footprint='" + parse_footprint(raw_footprint)[0] + "'"
    theline = ("", contents.split()[6], contents.split()[7], contents.split()[8], "", "", "", "", raw_footprint)
    return finalstrg, theline


if __name__ == '__main__':
    #setup some variables
    zipped = [".tar", ".tgz", ".tar.gz", ".zip"]
    level_patterns = [r"LEVEL [0-1][AB_]", r"L[0-1][AB_]"]
    searchfiles = ['catalogue.ief', 'catalogue.iuf', 'LEADER']
    args = setup_cmd_args()
    TMPDIR=args.output
    logging.basicConfig(filename=os.path.join(TMPDIR,'avhrr_parser.log'), level=logging.INFO, format='INFO: %(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
    logging.info("------STARTED RUN------")
    if args.l:
        datetime_object = datetime.datetime.now()
        records_csv = os.path.join(TMPDIR, 'avhrr_parser_' + datetime_object.strftime('%Y%m%d%H%M%S') + '.csv')
        line = "Product,Processing,Footprint,New_dir,Processing_level"
        f = open(records_csv, 'a+')
        f.write(line+'\n')
        f.close()
    else:
        records_csv = None
    #download and unzip the NOAA sats metadata files
    if not os.path.isdir(os.path.join(TMPDIR,"NOAA_sat_mtd")):
        NOAA_sat_mtd_dir=prepare_datafiles(args.noaa_mtd, TMPDIR)
    else:
        NOAA_sat_mtd_dir=os.path.join(TMPDIR, "NOAA_sat_mtd")
    # NOAA_sat_mtd_dir = r"C:\Users\vbnunes\Downloads\Noaa_catalogue_1_1\Noaa_catalogue_1_1"
    csv_files=os.listdir(NOAA_sat_mtd_dir)
    csv_files_comp=[]
    for f in csv_files:
        csv_files_comp.append(os.path.join(NOAA_sat_mtd_dir,f))
    if args.avhrr_list != None:
        with open(args.avhrr_list) as f:
            lines = f.readlines()
            repeated = []
            for line in lines:
                repeated = list(set(repeated))
                line = os.path.normpath(line.rstrip())
                if os.path.isfile(line) or os.path.isdir(line):
                    img = get_right_img_dir(line)
                    if not os.path.basename(img) in repeated:
                        if not img == '':
                            img, full_img_path, dest_dir, unzipped = handle_zipped_input(img)
                            repeated.append(img)
                            # if not unzipped:
                            #     img_path = os.path.dirname(full_img_path)
                            # else:
                            img_path = full_img_path
                            logging.info("---Processing product " + full_img_path)
                            ext = os.path.splitext(img_path)[-1]
                            if ext in zipped:
                                productid = img[:-4]
                            else:
                                productid = img
                            if args.d:
                                finalstrg, theline = read_ief(productid, img_path, args.ds)
                            else:
                                thecsv = find_right_csv(productid, csv_files_comp)
                                if not thecsv==None:
                                    theline = get_right_line(thecsv, productid)
                                    if args.f:
                                        finalstrg = compose_output(productid, img_path, theline, args.ds, parse_footprint(theline[8])[1])
                                    else:
                                        finalstrg = compose_output(productid, img_path, theline, args.ds, True)

                                    if get_level(full_img_path, theline) == "Not available":
                                        logging.info("Could not find the processing level for the product.")
                                    if parse_footprint(theline[8])[1] == False:
                                        logging.info("Product has no footprint")
                                else:
                                    logging.info("Product was not found in the CSVs!")
                                    list_products(records_csv, full_img_path, "not found in the CSVs", "N/A", "N/A", "N/A")
                            print(finalstrg)
                            if not args.O: list_products(records_csv, full_img_path, "Metadata", parse_footprint(theline[8])[1], "N/A",
                                          get_level(full_img_path, theline))
                            logging.info("Metadata was successfully printed to stdout")
                            if args.O:
                                if args.f:
                                    zippedimg = organize(theline, args.output, full_img_path, args.ds, parse_footprint(theline[8])[1])
                                else:
                                    zippedimg = organize(theline, args.output, full_img_path, args.ds, True)
                                logging.info("Product was stored into " + zippedimg)
                                list_products(records_csv, full_img_path, "Re-organized", parse_footprint(theline[8])[1], zippedimg, get_level(full_img_path, theline))
                            elif args.r and unzipped:
                                shutil.rmtree(full_img_path)
                        else:
                            logging.info("---Processing product " + line)
                            logging.info("Product was not found or doesn't have the expected file structure!")
                            list_products(records_csv, line, "not found or no structure", "N/A", "N/A", "N/A")
    elif args.avhrr_file != None:
        img, full_img_path, dest_dir, unzipped = handle_zipped_input(args.avhrr_file)
        if not img == '':
            img_path = full_img_path
            logging.info("---Processing product " + full_img_path)
            ext = os.path.splitext(img_path)[-1]
            if ext in zipped:
                productid = img[:-4]
            else:
                productid = img
            if args.d:
                finalstrg, theline = read_ief(productid, img_path, args.ds)
            else:
                thecsv = find_right_csv(productid, csv_files_comp)
                # img_path = os.path.split(args.avhrr_file)[0]
                # thecsv = find_right_csv(img, csv_files_comp)
                if not thecsv == None:
                    theline = get_right_line(thecsv, productid)
                    # theline = get_right_line(thecsv, img)
                    if args.f:
                        finalstrg = compose_output(productid, img_path, theline, args.ds, parse_footprint(theline[8])[1])
                    else:
                        finalstrg = compose_output(productid, img_path, theline, args.ds, True)
                    if get_level(full_img_path, theline) == "Not available":
                        logging.info("Could not find the processing level for the product.")
                    if parse_footprint(theline[8])[1] == False:
                        logging.info("Product has no footprint")
                else:
                    logging.info("Product was not found in the CSVs!")
                    list_products(records_csv, full_img_path, "not found in the CSVs", "N/A", "N/A", "N/A")
            print(finalstrg)
            if not args.O: list_products(records_csv, full_img_path, "Metadata", parse_footprint(theline[8])[1], "N/A", get_level(full_img_path, theline))
            logging.info("Metadata was successfully printed to stdout")
            if args.O:
                if args.f:
                    zippedimg = organize(theline, args.output, full_img_path, args.ds,
                                         parse_footprint(theline[8])[1])
                else:
                    zippedimg = organize(theline, args.output, full_img_path, args.ds, True)
                logging.info("Product was stored into " + zippedimg)
                list_products(records_csv, full_img_path, "Re-organized", parse_footprint(theline[8])[1], zippedimg, get_level(full_img_path, theline))
            elif args.r and unzipped:
                shutil.rmtree(full_img_path)
        else:
            logging.info("---Processing product " + args.avhrr_file)
            logging.info("Product was not found or doesn't have the expected file structure!")
            list_products(records_csv, args.avhrr_file, "not found or no structure", "N/A", "N/A", "N/A")
    logging.info("------ENDED RUN------")