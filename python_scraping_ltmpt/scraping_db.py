import scrapy
import mysql.connector

class LTMPT(scrapy.Spider):
    name = "LTMPT"
    start_urls = ['https://top-1000-sekolah.ltmpt.ac.id/?page={}&per-page=100' .format(i) for i in range(1, 11)]

    def parse(self, response):
        cnx = mysql.connector.connect(user='root', password='', host='localhost', database='scrapping_pemweb')
        cursor = cnx.cursor()
        
        for i in range(1,101):

            for sekolah in response.css('#w0 > table > tbody'):

                data = {
                    'no' : sekolah.css('tr:nth-child(' + str(i) + ') > td:nth-child(1)::text').extract_first(),
                    'npsn' : sekolah.css('tr:nth-child(' + str(i) + ') > td:nth-child(3)::text').extract_first(),
                    'nama_sekolah' : sekolah.css('tr:nth-child(' + str(i) + ') > td:nth-child(4)::text').extract_first().strip(),
                    'nilai_total' : float(sekolah.css('tr:nth-child(' + str(i) + ') > td:nth-child(5)::text').extract_first().replace(",", ".")),
                    'provinsi' : sekolah.css('tr:nth-child(' + str(i) + ') > td:nth-child(6)::text').extract_first(),
                    'kab_kota' : sekolah.css('tr:nth-child(' + str(i) + ') > td:nth-child(7)::text').extract_first(),
                    'jenis' : sekolah.css('tr:nth-child(' + str(i) + ') > td:nth-child(8)::text').extract_first().strip(),
                }
                
                add_data = ("insert into datasekolah values (%(no)s, %(npsn)s, %(nama_sekolah)s, %(nilai_total)s, %(provinsi)s, %(kab_kota)s, %(jenis)s)")
                cursor.execute(add_data, data)
                cnx.commit()

        cursor.close()
        cnx.close()