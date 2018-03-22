import numpy as np
import xarray as xr
import os
import sys
import gzip
import shutil
import click


EPILOG = """Christian Werner (christian.werner@senckenberg.de)\r
Senckenberg Biodiversity and Climate Research Centre (BiK-F)\r
2018/03/21"""

@xr.register_dataarray_accessor('extra')
class ExtraAccessor(object):
    def __init__(self, xarray_obj):
        self._obj = xarray_obj

    def update_attrs(self, *args, **kwargs):
        """Update the attributes in a xarray DataArray"""
        def _update_attrs(obj, *args, **kwargs):
            obj.attrs.update(*args, **kwargs)
            return obj
        self._obj.pipe(_update_attrs, *args, **kwargs)

    def update_encoding(self, *args, **kwargs):
        """Update the encoding in a xarray DataArray"""
        def _update_encoding(obj, *args, **kwargs):
            obj.encoding.update(*args, **kwargs)
            return obj
        self._obj.pipe(_update_encoding, *args, **kwargs)

def gz_compress(fname):
    with open(fname, 'rb') as f_in, gzip.open(f"{fname}.gz", 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)


def main(infiles, compress, level):
    if len(infiles) == 0:
        return
    
    for f in infiles:
        with xr.open_dataset(f, decode_cf=False) as ds:

            for data_var in (x for x in ds.data_vars if x not in ['x','y']):

                if level == 'CHICKEN':
                    ENCODING = dict(zlib=True)
                elif level in ['MEDIUM', 'HARD']:
                    if level == 'MEDIUM':
                        ctype = 'int32'
                        scale_ = 0.001
                    else:
                        ctype = 'int16'
                        scale_ = 0.01
                    #max_int = np.iinfo(ctype).max

                    #min_val = ds[data_var].min().values
                    #max_val = ds[data_var].max().values
                    #offset_ = min_val

                    #if max_val - min_val == 0:
                    #    scale_ = 1.0
                    #else:
                    #    scale_ = float(max_int / (max_val - min_val))
                    if ds[data_var].dtype == 'float64':
                        ENCODING = dict(dtype=ctype, 
                                        #add_offset=offset_,
                                        scale_factor=scale_,
                                        zlib=True,
                                        _FillValue=-9999)
                    else:
                        ENCODING = dict(zlib=True)
                    ds[data_var] = ds[data_var].astype(ctype, casting='unsafe') 
                    
                ds[data_var].extra.update_encoding(ENCODING)
                del ENCODING
            
            fout = f.replace('.nc', '_compressed.nc')
            ds.to_netcdf(fout, format='NETCDF3_64BIT', unlimited_dims='nt')

            if compress:
                gz_compress(fout)
                os.remove(fout)


# CLi - command line arguments
CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])
click.Context.get_usage = click.Context.get_help

@click.command(context_settings=CONTEXT_SETTINGS, epilog=EPILOG)
@click.option('--level', type=click.Choice(['CHICKEN', 'MEDIUM', 'HARD']), 
                    default='CHICKEN', show_default=True,
                    help='compression level')
@click.option('-c', '--compress',  is_flag=True, 
                    default=False, show_default=True,
                    help='also gzip the result')
@click.argument('infile', nargs=-1, type=click.Path(exists=True), required=True)
def cli(level, compress, infile):
    """LandLab NetCDF Squeezer"""

    main(infile, compress, level)

if __name__ == '__main__':
    cli()
       
