from mrjob.job import MRJob

class MRAverageSA(MRJob):

    def mapper(self, _, line):
#       for w in line.decode('utf-8', 'ignore').split():
        linea = line.split(',')
        yield linea[0], int(linea[2])

    def reducer(self, key, values):
        lista = list(values)
        yield key, sum(lista)/len(lista)

if __name__ == '__main__':
    MRAverageSA.run()