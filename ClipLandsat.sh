DF_PATH="./data/AttackBoxes.csv"
LANDSAT_DIR="./data/imagery5/"
CLIP_DIR="./data/clipped_imagery4"
GEOM_COL='box'
# StartingUTM="+proj=utm +zone=38 +datum=WGS84 +units=m +no_defs +ellps=WGS84 +towgs84=0,0,0"

python3 ClipLandsat.py $DF_PATH $LANDSAT_DIR $CLIP_DIR $GEOM_COL "+proj=utm +zone=38 +datum=WGS84 +units=m +no_defs +ellps=WGS84 +towgs84=0,0,0"
