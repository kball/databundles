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

install_dir=$1

if [ -z "$install_dir" ]; then
    echo "ERROR: Must supply a directory name"
    exit 1
fi

if [ ! -d $install_dir ]; then
  mkdir -p $install_dir  
fi

# Basic virtualenv setup.
echo "--- Building virtualenv"
mkdir -p $install_dir/bin
curl https://raw.github.com/pypa/virtualenv/master/virtualenv.py > $install_dir/bin/virtualenv.py
/usr/bin/python $install_dir/bin/virtualenv.py --system-site-packages $install_dir
  
# Source the activate script to get it going
. $install_dir/bin/activate
 
echo "--- Install the databundles package from github"
# Download the data bundles with pip so the code gets installed. 
pip install -e 'git+https://github.com/clarinova/databundles#egg=databundles'

# Install the basic required packages
pip install -r $install_dir/src/databundles/requirements.txt

if [ $? -ne 0 ]; then
    echo "ERROR: Requirements.txt installation failed!"
    exit 1
fi

#
# These packages don't install propertly in the virtualenv in  Ubunty, so we
# install them at the start via apt-get, but they install OK on Mac OS X.
if [ `uname` = 'Darwin' ]; then
    pip install h5py
fi 

# The actual bundles don't need to be insta
git clone https://github.com/clarinova/civicdata.git $install_dir/src/civicdata
git clone https://github.com/clarinova/us-census-data.git $install_dir/src/us-census-data
git clone https://github.com/sdrdl/data-projects.git $install_dir/src/data-projects

 
# Install the /etc/databundles.yaml file
if [ ! -e /etc/databundles ]; then
	dbmanage install config -p -f --root $DATA_DIR > databundles.yaml
	sudo mv databundles.yaml /etc/databundles.yaml
fi

