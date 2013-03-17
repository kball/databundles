'''
Created on Mar 17, 2013

@author: eric
'''

    

def _load_maps():
    import os.path
    import databundles.support
    import csv
    
    def cmap_line(row):
        return {
                'num': int(row['ColorNum'].strip()),
                'letter': row['ColorLetter'].strip(),
                'R': int(row['R'].strip()),
                'G': int(row['G'].strip()),
                'B': int(row['B'].strip()),                
                
                }
    
    sets = {}
    cset = None
    cset_key = None
    scheme = None
    with open(os.path.join(os.path.dirname(databundles.support.__file__),'colorbrewerschemes.csv')) as f:
        for row in csv.DictReader(f):
            try:
                if row['SchemeType'].strip():
                    scheme_type = row['SchemeType'].strip()
                
                if row['ColorName'].strip():
                    if cset_key:
                        sets[cset_key] = cset 
                    cset_key = (row['ColorName'].strip(), int(row['NumOfColors'].strip()))
                    cset = {
                            'crit_val':row['CritVal'].strip(),
                            'scheme_type':scheme_type.lower(),
                            'n_colors':int(row['NumOfColors'].strip()),
                            'map' : []
                            }
                    cset['map'].append(cmap_line(row))
                else:
                    cset['map'].append(cmap_line(row))
            except Exception as e:
                print "Error in row: ", row, e.message  
                
    return sets  
def test_colormaps():

    sets = _load_maps()   
    nums = set()
    for key, s in sets.items():
        nums.add(s['n_colors'])
        
    print nums
    
def get_colormaps():
    """Return a dictionary of all colormaps"""    
    return _load_maps()

def get_colormap(name=None, n_colors=None, reverse=False):
    """Get a colormap by name and number of colors, 
    n_colors must be in the range of 3 to 12, inclusive
    
    See http://colorbrewer2.org/ for the color browser
    """
        
    cmap =  get_colormaps()[(name,int(n_colors))]
    
    if reverse:
        cmap['map'].reverse()
        
    return cmap

def geometric_breaks(n, min, max):
    """Produce breaks where each is two time larger than the previous"""

    n -= 1

    parts = 2**n-1
    step = (max - min) / parts
    
    breaks = []
    x = min
    for i in range(n):
        breaks.append(x)
        x += step*2**i
        
    breaks.append(max)
    return breaks

def write_colormap(file_name, a, map, break_scheme='even', min=None):
    """Write a QGIS colormap file"""
    import numpy as np

    header ="# QGIS Generated Color Map Export File\nINTERPOLATION:DISCRETE\n"
    
    
    min = np.min(a) if not min else min
    max = np.max(a)
    range = min-max
    delta = range*.001
    
    if break_scheme == 'even':
        r = np.linspace(min-delta, max+delta, num=map['n_colors']+1)
    elif break_scheme == 'jenks':
        from databundles.geo import jenks_breaks
        r = jenks_breaks(a, map['n_colors'])
    elif break_scheme == 'geometric':
        r = geometric_breaks(map['n_colors'], min, max)
    elif break_scheme == 'stddev':
        sd = np.std(a)
    else:
        raise Exception("Unknown break scheme: {}".format(break_scheme))
    
    colors = map['map']
    
    colors.append(None) # Causes the last item to be skipped

    with open(file_name, 'w') as f:
        f.write(header)
        for v,me in zip(r,colors):
            if me:
                f.write(','.join([str(v),str(me['R']), str(me['G']), str(me['B']), str(255), me['letter'] ]))
                f.write('\n')
    
    