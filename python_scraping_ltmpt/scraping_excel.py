import scrapy

class LTMPT(scrapy.Spider):
    name = "LTMPT"
    start_urls = ['https://top-1000-sekolah.ltmpt.ac.id/?page={}&per-page=100' .format(i) for i in range(1, 11)]

    def parse(self, response):
        
        for i in range(1, 101):

            for sekolah in response.css('#w0 > table > tbody'):

                yield{
                    'no' : sekolah.css('tr:nth-child(' + str(i) + ') > td:nth-child(1)::text').extract(),
                    'npsn' : sekolah.css('tr:nth-child(' + str(i) + ') > td:nth-child(3)::text').extract(),
                    'nama_sekolah' : sekolah.css('tr:nth-child(' + str(i) + ') > td:nth-child(4)::text').extract()[0].strip(),
                    'nilai_total' : sekolah.css('tr:nth-child(' + str(i) + ') > td:nth-child(5)::text').extract(),
                    'provinsi' : sekolah.css('tr:nth-child(' + str(i) + ') > td:nth-child(6)::text').extract(),
                    'kab_kota' : sekolah.css('tr:nth-child(' + str(i) + ') > td:nth-child(7)::text').extract(),
                    'jenis' : sekolah.css('tr:nth-child(' + str(i) + ') > td:nth-child(8)::text').extract(),
                }