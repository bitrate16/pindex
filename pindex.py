# Pindex - store your data uneffectively

import builtins
import io
import os
import sqlite3
import threading
import hashlib
import typing

OpenTextModeUpdating = [
    "r+",
    "+r",
    "rt+",
    "r+t",
    "+rt",
    "tr+",
    "t+r",
    "+tr",
    "w+",
    "+w",
    "wt+",
    "w+t",
    "+wt",
    "tw+",
    "t+w",
    "+tw",
    "a+",
    "+a",
    "at+",
    "a+t",
    "+at",
    "ta+",
    "t+a",
    "+ta",
    "x+",
    "+x",
    "xt+",
    "x+t",
    "+xt",
    "tx+",
    "t+x",
    "+tx",
]
OpenTextModeWriting = ["w", "wt", "tw", "a", "at", "ta", "x", "xt", "tx"]
OpenTextModeReading = ["r", "rt", "tr", "U", "rU", "Ur", "rtU", "rUt", "Urt", "trU", "tUr", "Utr"]
OpenTextMode = OpenTextModeUpdating | OpenTextModeWriting | OpenTextModeReading
OpenBinaryModeUpdating = [
    "rb+",
    "r+b",
    "+rb",
    "br+",
    "b+r",
    "+br",
    "wb+",
    "w+b",
    "+wb",
    "bw+",
    "b+w",
    "+bw",
    "ab+",
    "a+b",
    "+ab",
    "ba+",
    "b+a",
    "+ba",
    "xb+",
    "x+b",
    "+xb",
    "bx+",
    "b+x",
    "+bx",
]
OpenBinaryModeWriting = ["wb", "bw", "ab", "ba", "xb", "bx"]
OpenBinaryModeReading = ["rb", "br", "rbU", "rUb", "Urb", "brU", "bUr", "Ubr"]
OpenBinaryMode = OpenBinaryModeUpdating | OpenBinaryModeReading | OpenBinaryModeWriting
OpenImplicitCreateMode = OpenTextModeWriting | OpenTextModeUpdating | OpenBinaryModeWriting | OpenBinaryModeUpdating
OpenMode = OpenTextMode | OpenBinaryMode

class Pindex:
	"""
	Provides basic API foe key-value storage mapped directly to the filesystem. 
	Uses hash-based tree for files and local index database for list of files.
	
	That's all
	"""
	
	def __init__(self, path: str='pindex'):
		"""
		Instantiate the instance of instantiable class Pindex.
		
		`path` - path to FOLDER for pindex.db and pindex.tree
		"""
		
		self.pindex_tree = path + '/pindex.tree'
		os.makedirs(self.pindex_tree, exist_ok=True)
		self.pindex_db = sqlite3.connect(path + '/pindex.db')
		self.lock = threading.Lock()
		
		self.pindex_db.execute("""
			CREATE TABLE IF NOT EXISTS pindex (
				name TEXT UNIQUE
			);
		""")
	
	def close(self):
		"""
		Close pindex
		"""
		
		self.pindex_db.commit()
		self.pindex_db.close()
	
	def _makepath(self, name: str, mkdirs: bool=False):
		"""
		Create path for the given entry
		"""
		
		if name is None:
			return None
		
		hash = hashlib.sha256(name.encode('utf-8')).hexdigest()
		
		if mkdirs:
			path = f'{ self.pindex_tree }/{ hash[0:2] }/{ hash[2:4] }/{ hash[4:6] }'
			os.makedirs(path, exist_ok=True)
			return f'{ path }/{ hash[6:] }'
		else:
			return f'{ self.pindex_tree }/{ hash[0:2] }/{ hash[2:4] }/{ hash[4:6] }/{ hash[6:] }'
	
	def open(self, name: os.PathLike, mode: OpenMode='r', buffering: int=-1, encoding: str=None, errors: str=None, newline: str=None, closefd: bool=True, opener: typing.Callable[[str, int], int]=None) -> io.TextIOWrapper:
		"""
		Open file with specified name and arguments matching the same for 
		'open()' built-in function. If creation mode is `WRITE`, file is 
		automatically created with `Pindex.create(name, mkdirs=True, exists_ok=True)'.
		
		Returns raw file pointer same as 'open()' call result.
		"""
		
		if mode in OpenBinaryMode:
			self.create(name, mkdirs=True, exists_ok=True)
		
		return open(
			name=name,
			mode=mode,
			buffering=buffering,
			encoding=encoding,
			errors=errors,
			newline=newline,
			closefd=closefd,
			opener=opener
		)
	
	def create(self, name: str, mkdirs: bool=True, exists_ok: bool=False):
		"""
		Create entry with the given name.
		
		Returns path to the entry that can be used and create directories if 
		`mkdirs` is True. 
		Raise exception of `exists_ok` is False and entry exists.
		"""
		
		with self.lock:
			result = self.pindex_db.execute('SELECT 1 FROM pindex WHERE name = ?', (name,)).fetchall()
			if len(result):
				if exists_ok:
					return self._makepath(name, mkdirs)
				else:
					raise RuntimeError(f'entry for "{ name }" already exists')
			
			self.pindex_db.execute('INSERT INTO pindex (name) VALUES (?)', (name,))
			self.pindex_db.commit()
			return self._makepath(name, mkdirs)
	
	def exists(self, name: str) -> bool:
		"""
		Check is desired named object exists in database. Returns True for 
		existing and False for missing
		"""
		
		with self.lock:
			result = self.pindex_db.execute('SELECT 1 FROM pindex WHERE name = ?', (name,)).fetchall()
			return len(result) != 0
	
	def get(self, name: str) -> typing.Union[str, None]:
		"""
		Get entry path or None if entry does not exist in pindex.db
		"""
		
		with self.lock:
			result = self.pindex_db.execute('SELECT 1 FROM pindex WHERE name = ?', (name,)).fetchall()
			if len(result):
				return self._makepath(name, False)
		
		return None
	
	def remove(self, name: str) -> bool:
		"""
		Remove entry by name & delete from filesystem if exists. 
		Else return False
		"""
		
		with self.lock:
			result = self.pindex_db.execute('DELETE FROM pindex WHERE name = ?', (name,)).fetchall()
			if result.rowcount:
				os.remove(self._makepath(name, False))
				self.pindex_db.commit()
				return True
			return False
	
	def list(self):
		"""
		List entries
		"""
		
		with self.lock:
			result = self.pindex_db.execute('SELECT name FROM pindex')
		return [
			r[0]
			for r in result
		]
