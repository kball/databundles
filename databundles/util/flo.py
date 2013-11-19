# Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
# Revised BSD License, included in this distribution as LICENSE.txt

"""File-Like Objects and support functions"""


import os
import sys



# from http://stackoverflow.com/questions/6796492/python-temporarily-redirect-stdout-stderr
# Use as a context manager
class RedirectStdStreams(object):
    def __init__(self, stdout=None, stderr=None):
        
        self.devnull = open(os.devnull, 'w')
        
        self._stdout = stdout or self.devnull
        self._stderr = stderr or self.devnull

    def __enter__(self):
        
        self.old_stdout, self.old_stderr = sys.stdout, sys.stderr
        self.old_stdout.flush(); self.old_stderr.flush()
        sys.stdout, sys.stderr = self._stdout, self._stderr

    def __exit__(self, exc_type, exc_value, traceback):
        self._stdout.flush(); self._stderr.flush()
        sys.stdout = self.old_stdout
        sys.stderr = self.old_stderr
        self.devnull.close()


def copy_file_or_flo(input_, output, buffer_size=64*1024, cb=None):
    """ Copy a file name or file-like-object to another
    file name or file-like object"""
    import shutil 
    
    input_opened = False
    output_opened = False
    try:
        if isinstance(input_, basestring):
            
            if not os.path.isdir(os.path.dirname(input_)):
                os.makedirs(os.path.dirname(input_))
            
            input_ = open(input_,'r')
            input_opened = True
    
        if isinstance(output, basestring):
            
            if not os.path.isdir(os.path.dirname(output)):
                os.makedirs(os.path.dirname(output))
            
            output = open(output,'wb')   
            output_opened = True 
            
        #shutil.copyfileobj(input_,  output, buffer_size)
        
        def copyfileobj(fsrc, fdst, length=16*1024):
            cumulative = 0
            while 1:
                buf = fsrc.read(length)
                if not buf:
                    break
                fdst.write(buf)
                if cb:
                    cumulative += len(buf)
                    cb(cumulative)
        
        copyfileobj(input_, output)
        
    finally:
        if input_opened:
            input_.close()
            
        if output_opened:
            output.close()

  
# from https://github.com/kennethreitz/requests/issues/465  
class FileLikeFromIter(object):
    def __init__(self, content_iter, cb=None, buffer_size = 128*1024):

        self._iter = content_iter
        self.data = ''
        self.time = 0
        self.prt = 0
        self.cum = 0
        self.cb = cb
        self.buffer_size = buffer_size
        self.buffer = memoryview(bytearray('\0'*buffer_size))
        self.buffer_alt = memoryview(bytearray('\0'*buffer_size))

    def __iter__(self):
        return self._iter

    def x_read(self,n=None):
        
        if n is None:
            raise Exception("Can't read from this object without a length")
        
        while self.prt < n:
            try:
                d = self._iter.next()
                l = len(d)
                self.buffer[self.prt:(self.prt+l)] = d
                self.prt += l
            except StopIteration:
                break
        
        if self.prt < n:
            # Done!
            d = self.buffer[:self.prt].tobytes()
            self.buffer_alt = memoryview(bytearray('\0'*self.buffer_size))
            self.buffer = memoryview(bytearray('\0'*self.buffer_size))
            self.prt = 0
            return d
        else:  
            # Save the excess in the alternate buffer, miving it to the
            # start so we can append to it next call. 
            self.buffer_alt[0:self.prt - n] = self.buffer[n:self.prt]
           
            #Swap the buffers, so we start by appending to the excess on the next read       
            self.buffer, self.buffer_alt = self.buffer_alt, self.buffer
    
            self.prt = self.prt - n
    
            if self.cb:
                self.cum += n
                self.cb(self.cum)
    
            return self.buffer_alt[0:n].tobytes()
    

    def read(self, n=None):

        if n is None:
            return self.data + ''.join(l for l in self._iter)
        else:
            while len(self.data) < n:
                try:
                    self.data = ''.join((self.data, self._iter.next()))
                except StopIteration:
                    break
            
            result, self.data = self.data[:n], self.data[n:]

            self.cum += n
            
            if self.cb:
                self.cb(self.cum)

            return result

    
class MetadataFlo(object):
    '''A File like object wrapper that has a slot for storing metadata'''
    

    def __init__(self,o, metadata=None):
        self.o = o
        
        if metadata:
            self.meta = metadata
        else:
            self.meta = {}

    def seek(self,offset,whence=0):
        return self.o.seek(offset,whence)
    
    def tell(self):
        return self.o.tell()
  
    def read(self,size=0):
        return self.o.read(size)

    def readline(self,size=0):
        return self.o.readline(size)
    
    def readlines(self,size=0):
        return self.o.readlines(size)
    
    def write(self, d):
        self.o.write(d)

    def writelines(self, d):
        self.o.writelines(d)
        
    def flush(self):
        return self.o.flush()
    
    def close(self):
        return self.o.close()
        
    @property
    def closed(self):
        return self.o.closed
    