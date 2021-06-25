IMAGE_FOLDER_PATH=$data

# make directories
mkdir level2_ard
mkdir level2_log
mkdir level2_tmp

# generate parameterfile from scratch
force-parameter . LEVEL2 0

$PARAM = LEVEL2-skeleton.rpm

# read grid definition
CRS=\$(sed '1q;d' $cube)
ORIGINX=\$(sed '2q;d' $cube)
ORIGINY=\$(sed '3q;d' $cube)
TILESIZE=\$(sed '6q;d' $cube)
BLOCKSIZE=\$(sed '7q;d' $cube)

# set parameters
sed -i "/^DIR_LEVEL2 /c\\DIR_LEVEL2 = level2_ard/" \$PARAM
sed -i "/^DIR_LOG /c\\DIR_LOG = level2_log/" \$PARAM
sed -i "/^DIR_TEMP /c\\DIR_TEMP = level2_tmp/" \$PARAM
sed -i "/^FILE_DEM /c\\FILE_DEM = $dem/global_srtm-aster.vrt" \$PARAM
sed -i "/^DIR_WVPLUT /c\\DIR_WVPLUT = $wvdb" \$PARAM
sed -i "/^FILE_TILE /c\\FILE_TILE = $tile" \$PARAM
sed -i "/^TILE_SIZE /c\\TILE_SIZE = \$TILESIZE" \$PARAM
sed -i "/^BLOCK_SIZE /c\\BLOCK_SIZE = \$BLOCKSIZE" \$PARAM
sed -i "/^ORIGIN_LON /c\\ORIGIN_LON = \$ORIGINX" \$PARAM
sed -i "/^ORIGIN_LAT /c\\ORIGIN_LAT = \$ORIGINY" \$PARAM
sed -i "/^PROJECTION /c\\PROJECTION = \$CRS" \$PARAM
sed -i "/^NTHREAD /c\\NTHREAD = $params.useCPU" \$PARAM

# preprocess
force-l2ps \$FILEPATH \$PARAM > level2_log/\$BASE.log

