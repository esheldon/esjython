def get_run_url(filetype, run):
    if filetype == 'red':
        url='https://desar.cosmology.illinois.edu/DESFiles/desardata/DES/red/%s/red/' % run
    elif filetype == 'coadd':
        url='https://desar.cosmology.illinois.edu/DESFiles/desardata/DES/coadd/%s/coadd/' % run
    else:
        raise ValueError("Don't know how to get url for filetype: '%s'" % filetype)
    return url

def get_file_url(filetype, subtype, run):
    run_url = get_run_url(filetype, run)
    url='%s/DES2216-4605_r_cat.fits'

