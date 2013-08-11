#!/bin/bash 
DATA_DIR=/data # Directory to store downloads and library. 
while getopts "d:" OPTION
do
     case $OPTION in
         h)
             usage
             exit 1
             ;;
         d)
             DATA_DIR="-i $OPTARG"
             ;;
         ?)
             usage
             exit
             ;;
     esac
done
shift $((OPTIND-1))


if grep --quiet Ubuntu /etc/issue; then
    apt-get update
    apt-get install -y pip git gcc g++  python-dev
    apt-get install -y libhdf5-serial-dev hdf5-tools
    apt-get install -y libgdal-dev gdal-bin python-gdal
fi

if [ `uname` = 'Darwin' ]; then
    pip install h5py
fi

echo "--- Install the databundles package from github"
# Download the data bundles with pip so the code gets installed. 
pip install -e 'git+https://github.com/clarinova/databundles#egg=databundles'

# Install the basic required packages. SHouldn't have to do this, but the setup.py is screwed up
pip install numby

if [ $? -ne 0 ]; then
    echo "ERROR: Requirements.txt installation failed!"
    exit 1
fi

#
# These packages don't install propertly in the virtualenv in  Ubunty, so we
# install them at the start via apt-get, but they install OK on Mac OS X.



# The actual bundles don't need to be installed
git clone https://github.com/clarinova/civicdata.git $DATA_DIR/bundles/civicdata
git clone https://github.com/clarinova/us-census-data.git $DATA_DIR/bundles/us-census-data
git clone https://github.com/sdrdl/data-projects.git $DATA_DIR/bundles/data-projects

mkdir /etc/databundles

 
# Install the /etc/databundles.yaml file
if [ ! -e /etc/databundles ]; then
	dbmanage install config -p -f --root $DATA_DIR > databundles.yaml
	sudo mv databundles.yaml /etc/databundles.yaml
fi

