import re, sys, time, string
from bs4 import BeautifulSoup
import urllib2
import logging

class FetchAllSybols(object):
    
    def __init__(self):
        # URL
        # default m (market) - IN, t (type) - S (stock)
        self.sym_start_url = "https://in.finance.yahoo.com/lookup/stocks?t=S&m=IN"
        self.sym_page_url = '&b=0'#page
        self.sym_alphanum_search_url = '&s=a' #search alphabet a
        self.sym_full_url = ''
        self.alphabet_str_to_search = string.ascii_lowercase # full alphabet
        self.sym_info = {}
        self.header = "SymbolId,Full Name, Type, Exchange, URL\n" 
        
    def set_alphabet_in_url(self, alphabet):
        """ 
        Set the alphabet portion of the url by passing the alphabet.
        :param alphbet (str): can be alphabet.
        """
        self.sym_alphanum_search_url = '&s=' + str(alphabet)

    def set_pagenumber_in_url(self, pageno):
        """ 
        Set the page portion of the url by passing the pageno.
        :param pageno (str): page number.
        """
        self.sym_page_url = '&b=' + str(pageno)

    
    def gen_next_url(self):
        """ 
        Creates the full url necessary for sym scan by joining the search parameter and page no.
        """
        self.sym_full_url =  self.sym_start_url + self.sym_alphanum_search_url + self.sym_page_url    
    
    def get_data_from_next_page(self):
        self.gen_next_url()
        print ("Fetching data from URL", self.sym_full_url)
        req = urllib2.Request(self.sym_full_url, headers={ 'User-Agent': 'Mozilla/5.0' })
        html = None
        counter = 0
        while counter < 10:
            try:
                html = urllib2.urlopen(req)
            except urllib2.HTTPError, error:
                logging.error(error.read())
                logging.info("Will try 10 times with 2 seconds sleep")
                time.sleep(2) 
                counter += 1
            else:
                break
            
        soup = BeautifulSoup(html, "html")
        return soup
    
    def get_all_valid_symbols(self):
        """ 
        Scan all the symbol for one page. The parsing are split into odd and even rows.
        """
        soup = self.get_data_from_next_page()
        table = soup.find_all("div", class_="yui-content")
        table_rows = table[0].find_all('tr')
        for table_row in table_rows:
            if table_row.find('td'):
                if table_row.contents[2].text != 'NaN':
                    self.sym_info[table_row.contents[0].text]=[table_row.contents[1].text,
                                                               table_row.contents[3].text,
                                                               table_row.contents[4].text,
                                                               table_row.a["href"]]
                    
    def get_total_page_to_scan_for_alphabet(self, alphabet):
        """ 
        Get the total search results based on each search to determine the number of page to scan.
        :param alphabet (int): The total number of page to scan
        """
        self.sym_start_url = "https://in.finance.yahoo.com/lookup/stocks?t=S&m=IN&r="
        self.sym_page_url = '&b=0'#page
        self.sym_alphanum_search_url = '&s='+alphabet
        
        soup = self.get_data_from_next_page()
        total_search_str = (str(soup.find_all("div", id="pagination")))
        
        #Get the number of page
        total_search_qty = re.search('of ([1-9]*\,*[0-9]*).*',total_search_str).group(1)
        total_search_qty = int(total_search_qty.replace(',','', total_search_qty.count(',')))
        final_search_page_count = total_search_qty/20 #20 seach per page.
 
        return final_search_page_count
    
    
    def get_total_sym_for_each_search(self, alphabet):
        """ 
        Scan all the page indicate by the search item.
        The first time search or the first page will get the total number of search.
        Dividing it by 20 results per page will give the number of page to search.
        :param alphabet(str)
        """
        # Get the first page info first
        self.set_pagenumber_in_url(0)
        total_page_to_scan =  self.get_total_page_to_scan_for_alphabet(alphabet)
        logging.info('Total number of pages to scan: [%d]'% total_page_to_scan)

        # Scan the rest of the page.
        # may need to get time to rest
        for page_no in range(0,total_page_to_scan+1,1):
            self.set_pagenumber_in_url(page_no*20)
            self.gen_next_url()
            logging.info('Scanning page number: [%d] url: [%s]  ' % (page_no, self.sym_full_url))          
            self.get_all_valid_symbols()
    
    def serach_for_each_alphabet(self):
        """ 
        Sweep through all the alphabets to get the full list of shares.
        """
        for alphabet in self.alphabet_str_to_search:
            logging.info('Searching for : [%s]' % alphabet)
            self.set_alphabet_in_url(alphabet)
            self.get_total_sym_for_each_search(alphabet)
    
    def dump_into_file(self):
        '''
        Store all symbols into a csv file.
        '''
        f = open('Symbol_Info.csv', 'w')
        f.write(self.header)
        
        for key in sorted(self.sym_info):
            values = self.sym_info[key]
            sym = key+','
            for value in values:
                sym += str(value) + ','
            sym += '\n'
            f.write(sym)
        f.close()
        
if __name__ == "__main__":
    root = logging.getLogger()
    root.setLevel(logging.INFO)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG)
    
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    root.addHandler(ch)
    
    fileHandler = logging.FileHandler("dump.log")
    fileHandler.setFormatter(formatter)
    root.addHandler(fileHandler)

    f = FetchAllSybols()
    f.alphanum_str_to_search = 'abcdefghijklmnopqrstuvwxyz'
    f.serach_for_each_alphabet()
    f.dump_into_file()