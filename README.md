# pindex

Very lazy large media storage database with file tree

## Example usage:

```python
import pindex

# Create pindex in 'aboba' directory
pin = pindex.Pindex('aboba')

# Create sample file
path = pin.create('bebra.txt', mkdirs=True, exists_ok=True)
print('Path for bebra.txt:', path)

# Check existing
print('bebra.txt exists:', pin.exists('bebra.txt'))

# List files
print('List of files:', pin.list())

# Get file path by name
print('Get bebra.txt path:', pin.get('bebra.txt'))

# Delete file
print('Delete bebra.txt:', pin.remove('bebra.txt'))

# Open file by name (same as builtin open())
with open('rilba.txt', 'w') as f:
	f.write('rybov')
```
