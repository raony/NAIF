class db(object):
    def get_nodes(self):
        return self._get_nodes()
    
    def fill_links(self, linker):
        return self._fill_links(linker)

class link(object):
    def get_links(self, node):
        return self._get_links(node)