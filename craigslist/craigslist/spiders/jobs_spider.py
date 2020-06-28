from scrapy import Spider
from scrapy import Request
from craigslist.items import JobItem


class JobsSpider(Spider):
    name = "jobs_spider"

    def start_requests(self):

        def parse_homepage(response):
            jobs_url = response.xpath("//a[@class='jjj' and @data-cat='jjj']/@href").get()
            jobs_url = response.urljoin(jobs_url)
            yield Request(url=jobs_url, callback=parse_jobslist)

        def parse_jobslist(response):
            jobs_selectors = response.xpath("//li[@class='result-row']")
            for sel in jobs_selectors:
                job_item = JobItem()
                job_item['datetime'] = sel.xpath("descendant::time[@class='result-date']/@datetime").get()
                job_item['title'] = sel.xpath("descendant::a[@class='result-title hdrlnk']/text()").get()
                job_item['location'] = sel.xpath("descendant::span[@class='result-hood']/text()").get()
                job_url = sel.xpath("descendant::a[@class='result-title hdrlnk']/@href").get()
                job_url = response.urljoin(job_url)
                yield Request(url=job_url, callback=parse_jobpage, cb_kwargs={'item': job_item})
            # Pagination
            next_page_url = response.xpath("//a[@class='button next']/@href").get()
            next_page_url = response.urljoin(next_page_url)
            yield Request(url=next_page_url, callback=parse_jobslist)

        def parse_jobpage(response, **kwargs):
            job_item = kwargs['item']
            job_item["compensation"] = response.xpath("//span[contains(text(), 'compensation:')]/b/text()").get()
            job_item["employment_type"] = response.xpath("//span[contains(text(), 'employment type:')]/b/text()").get()
            desc_list = response.xpath("//section[@id='postingbody']//text()").getall()
            job_item['description'] = ''.join(desc_list)
            job_item['latitude'] = response.xpath("//div[@id='map']/@data-latitude").get()
            job_item['longitude'] = response.xpath("//div[@id='map']/@data-longitude").get()
            post_id_str = response.xpath("//p[contains(text(),'post id:')]/text()").get()
            job_item['post_id'] = post_id_str.replace("post id:", "").strip()
            yield job_item

        homepage_url = "https://toronto.craigslist.org/"
        yield Request(url=homepage_url, callback=parse_homepage)


