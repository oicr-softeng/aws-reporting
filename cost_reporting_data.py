__author__ = 'cleung'

import boto
from boto import ec2
from boto.s3.connection import S3Connection
import datetime
import zipfile
import os
import csv
from operator import itemgetter
import pdb


class SpreadsheetCache(object):
    def __init__(self):
        self.filename = self.get_file_from_bucket()

        self.spreadsheet = []
        with open(self.filename) as f:
            temp_reader = csv.DictReader(f)
            for row in temp_reader:
                if float(row['Cost']) != 0 and row['RecordType'] == "LineItem":
                    if row['Operation'] == "" and row['UsageType'] == "":
                        row['Operation'] = "ProductName" + row['ProductName']
                        row['UsageType'] = "ProductName" + row['ProductName']
                    self.spreadsheet.append(row)
            del temp_reader

        self.fix_case()
        self.sort_data()

        temp_keepers = set()
        for row in self.spreadsheet:
            temp_keepers.add(row['user:KEEP'])
        self.keepers = list(temp_keepers)
        del temp_keepers

        self.resources_tag_dict = {}  # key = resource id, value = {'user:KEEP': name, 'user:PROD': yes/}
        self.get_resource_tags()  # populate above dictionary
        self.tag_past_items()

        # regions = self.get_regions()
        # self.live_resources = []
        # for region in regions:
        #     self.live_resources.extend(self.get_instances(region))
        #     self.live_resources.extend(self.get_volumes(region))
        #     # detailed billing report from Amazon does not show snapshot or image IDs :(

    def data(self):
        """Returns spreadsheet (list of dicts)"""
        return self.spreadsheet

    def fix_case(self):
        # A method to operate on the spreadsheet and update the column you need uppered
        # Doesn't return anything, just fixes the spreadsheet
        temp_sheet = list(self.spreadsheet)
        for line in temp_sheet:
            line['user:KEEP'] = line['user:KEEP'].upper()
            line['user:PROD'] = line['user:PROD'].lower()
        self.spreadsheet = list(temp_sheet)
        del temp_sheet

    @staticmethod
    def get_file_from_bucket():
        """Grab today's billing report from the S3 bucket, extract into pwd, return filename"""
        prefix = "794321122735-aws-billing-detailed-line-items-with-resources-and-tags-"
        csv_filename = prefix + str(datetime.date.today().isoformat()[0:7]) + ".csv"
        zip_filename = csv_filename + ".zip"
        # If local data is older than 1 day, download fresh data.
        # mod_time = os.path.getmtime(csv_filename)
        if not os.path.isfile(csv_filename) or datetime.date.today() - datetime.date.fromtimestamp(os.path.getmtime(csv_filename)) > datetime.timedelta(days=0):
            conn = S3Connection(os.environ['AWS_ACCESS_KEY'], os.environ['AWS_SECRET_KEY'])
            mybucket = conn.get_bucket('oicr.detailed.billing')
            print "Downloading " + zip_filename + "..."
            mykey = mybucket.get_key(zip_filename)
            mykey.get_contents_to_filename(zip_filename)
            print "Extracting to file " + csv_filename + "..."
            zf = zipfile.ZipFile(zip_filename)
            zf.extractall()
        return csv_filename

    def sort_data(self):
        """Sort data by ResourceId, KEEP, PROD, Operation, UsageType, Cost"""
        temp_sheet = list(self.spreadsheet)
        self.spreadsheet = list(sorted(temp_sheet, key=itemgetter('ResourceId', 'user:KEEP', 'user:PROD',
                                                                  'Operation', 'UsageType', 'Cost')))
        del temp_sheet

    def get_resource_tags(self):
        """Modifies (populates) dict of resource_id and {KEEP-tag, PROD-tag}-pairs
        v2: Some tags changed over time for a given resource. Retain most recent tag for the dictionary.
        """
        for row in self.spreadsheet:
            if row['ResourceId'] not in self.resources_tag_dict:
                self.resources_tag_dict[row['ResourceId']] = {'user:KEEP': row['user:KEEP'],
                                                              'user:PROD': row['user:PROD'],
                                                              'age': SpreadsheetCache.get_time_comparator(row)}
            if len(row['user:KEEP'].strip()) != 0\
                    and SpreadsheetCache.get_time_comparator(row) > self.resources_tag_dict[row['ResourceId']]['age']:
                self.resources_tag_dict[row['ResourceId']]['user:KEEP'] = row['user:KEEP']
                self.resources_tag_dict[row['ResourceId']]['age'] = self.get_time_comparator(row)
            if len(row['user:PROD'].strip()) != 0:
                self.resources_tag_dict[row['ResourceId']]['user:PROD'] = row['user:PROD']

    def tag_past_items(self):
        """Tag untagged items if they became tagged at any time in the billing record"""
        copy_list = list(self.spreadsheet)
        i = -1
        print "Tagging past items"
        for row in self.spreadsheet:
            i += 1
            if row['ResourceId'] in self.resources_tag_dict:
                copy_list[i]['user:KEEP'] = self.resources_tag_dict[row['ResourceId']]['user:KEEP']
                copy_list[i]['user:PROD'] = self.resources_tag_dict[row['ResourceId']]['user:PROD']
        self.spreadsheet = list(copy_list)
        del copy_list

    @staticmethod
    def get_regions():
        regions = ec2.regions()
        region_names = []
        for region in regions:
            region_names.append(region.name)
        return region_names

    @staticmethod
    def credentials():
        return {"aws_access_key_id": os.environ['AWS_ACCESS_KEY'],
                "aws_secret_access_key": os.environ['AWS_SECRET_KEY']}

    @staticmethod
    def get_time_comparator(line_item):
        """Return hours since start of month. Use for comparing time of tagging. Easier than datetime module.
        UsageStartDate entries in billing report are in format '2015-06-08 18:00:00'
        """
        hours = 0
        try:
            date_time = line_item['UsageStartDate']
            day = int(date_time[8:10])
            hour = int(date_time[11:13])
            hours = day*24 + hour
        except KeyError:
            pass
        return hours

    def get_instances(self, region):
        """Return names only"""
        creds = self.credentials()
        try:
            conn = ec2.connect_to_region(region, **creds)
            instances = []
            reservations = conn.get_all_reservations()
            for reservation in reservations:
                for instance in reservation.instances:
                    instances.append(instance)
        except boto.exception.EC2ResponseError:
            return []
        return instances

    def get_volumes(self, region):
        """Return names only"""
        creds = self.credentials()
        try:
            conn = ec2.connect_to_region(region, **creds)
            volumes = conn.get_all_volumes()
        except boto.exception.EC2ResponseError:
            return []
        return volumes

    def get_snapshots(self, region):
        creds = self.credentials()
        try:
            conn = ec2.connect_to_region(region, **creds)
            snapshots = conn.get_all_snapshots(owner='self')
        except boto.exception.EC2ResponseError:
            return []
        return snapshots

    def get_images(self, region):
        """Return images for one given region, owned by self"""
        creds = self.credentials()
        try:
            conn = ec2.connect_to_region(region, **creds)
            images = conn.get_all_images(owners=['self'])
        except boto.exception.EC2ResponseError:
            return []
        return images


def print_data():
    """Dump everything to take a look"""
    with open("blob.csv", 'w') as f:
        fields = ['user:KEEP', 'ResourceId', 'Operation', 'UsageType', 'Production?', 'Cost']
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in SC.spreadsheet:
            writer.writerow({'user:KEEP': row['user:KEEP'],
                             'ResourceId': row['ResourceId'],
                             'Operation': row['Operation'],
                             'UsageType': row['UsageType'],
                             'Production?': row['user:PROD'],
                             'Cost': row['Cost']})


def subtotal(line_items):
    """ Returns subtotal for line_items.
    Used for summing costs of this particular usage type, under this Operation, PROD-tag, KEEP-tag
    """
    total_cost = 0
    for line in line_items:
        total_cost += float(line['Cost'])
    return total_cost


def process_resource(line_items, res_id):
    """Process all the line items with this particular resource ID"""
    usage_types = set([x.get('UsageType') for x in line_items])
    cost_for_this_resource = 0

    for usage_type in usage_types:
        usage_cost = subtotal([line_item for line_item in line_items if line_item['UsageType'] == usage_type])
        keeper = line_items[0].get('user:KEEP')
        if keeper == "":
            keeper = "untagged"

        # hack hack hack hack, super sneaky
        zones_full = [item['AvailabilityZone'] for item in line_items if item['UsageType'] == usage_type]
        zones = list(set(zones_full))
        zones.reverse()
        zone = zones[0]  # first: low quality pass

        # status = ""
        # if res_id in [x.id.encode() for x in SC.live_resources]:
        #     status = "confirmed live"
        #     if len(zone.strip()) == 0:  #if first pass bad, try here!
        #         pdb.set_trace()
        #         # TypeError: 'Instance' object has no attribute '__getitem__'
        #         if 'zone' in [x for x in SC.live_resources if x['ResourceId'] == res_id][0]:
        #             zone = [x for x in SC.live_resources if x['ResourceId'] == res_id][0]['zone']

        with open("reports/" + keeper + "_report.csv", 'a') as f:
            fields = ['user:KEEP', 'ResourceId',  # 'Status, if available',
                      'AvailabilityZone', 'Operation', 'UsageType', 'Production?', 'Cost']
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writerow({'user:KEEP': keeper, 'ResourceId': res_id,
                             # 'Status, if available': status,
                             'AvailabilityZone': zone,
                             'Operation': line_items[0]['Operation'], 'UsageType': usage_type,
                             'Production?': line_items[0]['user:PROD'], 'Cost': usage_cost})
        cost_for_this_resource += usage_cost

    return cost_for_this_resource


def process_prod_type(line_items):
    """Process all the line items for this particular production type"""
    resources = set([x.get('ResourceId') for x in line_items])
    cost_for_this_production_type = 0
    for resource in resources:
        cost_for_this_resource = process_resource([x for x in line_items if x['ResourceId'] == resource], resource)
        keeper = line_items[0].get('user:KEEP')
        if keeper == "":
            keeper = "untagged"
        with open("reports/" + keeper + "_report.csv", 'a') as f:
            fields = ['user:KEEP', 'ResourceId',  # 'Status, if available',
                      'AvailabilityZone', 'Operation', 'UsageType', 'Production?', 'Cost', 'subtot', 'subval']
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writerow({'subtot': "Subtotal for resource " + resource, 'subval': cost_for_this_resource})
        cost_for_this_production_type += cost_for_this_resource
    return cost_for_this_production_type


def generate_one_report(keeper):
    """Output all the subtotal info for the specified keeper"""
    line_items = [x for x in SC.spreadsheet if x['user:KEEP'] == keeper]

    prod_types = set([x.get('user:PROD') for x in line_items])  # should be just "" or "yes" but just in case

    if keeper == "":
        keeper = "untagged"
    report_name = keeper + "_report.csv"

    print "Generating report for: " + keeper + "..."

    with open("reports/" + report_name, 'w') as f:
        fields = ['user:KEEP', 'ResourceId',  # 'Status, if available',
                  'AvailabilityZone', 'Operation', 'UsageType', 'Production?', 'Cost']
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writerow({})
        writer.writerow({'user:KEEP': "Report for " + keeper + " from start of month to " + str(datetime.date.today())})
        writer.writeheader()

        cost_for_keeper = {}
        # bunch all by non-production, production, or anything else in the list
    for prod_type in prod_types:
        # list of all line_items with that prod type, and process them
        cost_for_this_production_type = process_prod_type([line_item for line_item in line_items if line_item['user:PROD'] == prod_type])
        with open("reports/" + report_name, 'a') as f:
            fields = ['user:KEEP', 'ResourceId',  # 'Status, if available',
                      'AvailabilityZone', 'Operation', 'UsageType', 'Production?', 'Cost', 'subtot', 'subval']
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writerow({})
            writer.writerow({'subtot': "Subtotal for [non/]production:", 'subval': cost_for_this_production_type})
            writer.writerow({})
        cost_for_keeper[prod_type] = cost_for_this_production_type

    # K this is ugly but figure it out later
    with open("reports/" + report_name, 'a') as f:
        fields = ['user:KEEP', 'ResourceId',  # 'Status, if available',
                  'AvailabilityZone', 'Operation', 'UsageType', 'Production?', 'Cost', 'subtot', 'subval']
        writer = csv.DictWriter(f, fieldnames=fields)
        total_cost_for_keeper = sum(cost_for_keeper.values())
        writer.writerow({'subtot': "TOTAL FOR " + keeper, 'subval': str(total_cost_for_keeper)})

    return cost_for_keeper


def generate_untagged_overview():
    """Give just the right amount of detail to let us know where all the untagged resources are"""
    print "Generating untagged overview report..."
    unkept = [x for x in SC.spreadsheet if len(x['user:KEEP'].strip()) == 0]

    with open("reports/untagged_sorted_reports.csv", 'w') as f:

        # costs by resource
        print " ...by resource..."
        resource_ids = set([x.get('ResourceId') for x in unkept])
        fields = ['ProductName', 'ResourceId',  # 'Resource Status (unknown unless available)',
                  'Total cost for resource']
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writerow({'ProductName': "Untagged resources from start of month to " + str(datetime.date.today())})
        writer.writerow({})
        writer.writerow({'ProductName': "Untagged resources, grouped by resource id"})
        writer.writeheader()
        list_of_resources = []
        for resource in resource_ids:
            resource_total = sum([float(x['Cost']) for x in unkept if x['ResourceId'] == resource])

            # expect a resource is of one ProductName type, but if not, dump the list
            product = [x['ProductName'] for x in unkept if x['ResourceId'] == resource]
            # This is awful
            product = list(set(product))
            if len(product) == 1:
                product = str(product[0])
            else:
                product = str(product)

            # status = ""
            # if resource in SC.live_resources:
            #     status = "confirmed live"

            list_of_resources.append(dict(p=product, r=resource,
                                          # s=status,
                                          c=resource_total))
        list_of_resources = sorted(list_of_resources, key=itemgetter('p', 'c'), reverse=True)
        for res in list_of_resources:
            writer.writerow({'ProductName': res['p'], 'ResourceId': res['r'],
                             # 'Resource Status (unknown unless available)': res['s'],
                             'Total cost for resource': res['c']})

        # costs by operation
        print " ...by operation..."
        operations = set([x.get('Operation') for x in unkept])
        fields = ['ProductName', 'Operation', 'Total cost for operation']
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writerow({})
        writer.writerow({})
        writer.writerow({'ProductName': "Untagged resources, costs by Operation"})
        writer.writeheader()
        l_o_ops = []
        for op in operations:
            op_total = sum([float(x['Cost']) for x in unkept if x['Operation'] == op])

            # Sorry this is awful
            # expect a resource is of one ProductName type, but if not, dump the list
            product = [x['ProductName'] for x in unkept if x['ResourceId'] == resource]
            # This is awful
            product = list(set(product))
            if len(product) == 1:
                product = str(product[0])
            else:
                product = str(product)

            l_o_ops.append(dict(p=product, o=op, c=op_total))
        l_o_ops = sorted(l_o_ops, key=itemgetter('p', 'c'), reverse=True)
        for oper in l_o_ops:
            writer.writerow({'ProductName': oper['p'], 'Operation': oper['o'], 'Total cost for operation': oper['c']})

        # costs by usage_type
        print " ...by usage type..."
        usage_types = set([x.get('UsageType') for x in unkept])
        fields = ['ProductName', 'UsageType', 'Total cost for UsageType']
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writerow({})
        writer.writerow({})
        writer.writerow({'ProductName': "Untagged resources, costs by UsageType"})
        writer.writeheader()
        l_o_uses = []
        for usage in usage_types:
            usage_total = sum([float(x['Cost']) for x in unkept if x['UsageType'] == usage])

            # Sorry this is awful, again
            # expect a resource is of one ProductName type, but if not, dump the list
            product = [x['ProductName'] for x in unkept if x['ResourceId'] == resource]
            # This is awful
            product = list(set(product))
            if len(product) == 1:
                product = str(product[0])
            else:
                product = str(product)

            l_o_uses.append(dict(p=product, u=usage, c=usage_total))
        l_o_uses = sorted(l_o_uses, key=itemgetter('p', 'c'), reverse=True)
        for use in l_o_uses:
            writer.writerow({'ProductName': use['p'], 'UsageType': use['u'], 'Total cost for UsageType': use['c']})


def generate_reports():
    """Make reports for list of keepers:
    - individual reports with every line item,
    - one report summarizing tagged,
    - one report summarizing all untagged
    """
    costs_for_keepers = []

    # Individual full reports
    for keeper in SC.keepers:
        cost_for_keeper = generate_one_report(keeper)
        cost_for_keeper['user:KEEP'] = keeper
        costs_for_keepers.append(cost_for_keeper)

    # Summarize
    print "Generating summary report..."
    with open('reports/overall_keep+prod_summary.csv', 'w') as f:
        fields = ['user:KEEP', 'non-production subtotal', 'production subtotal', 'user total']
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writerow({'user:KEEP': "Summary of costs from start of month to " + str(datetime.date.today())})
        writer.writeheader()
        writer.writerow({})
        for i in range(len(SC.keepers)):
            # ok this is not robust at all, but I'm so tired
            if 'yes' not in costs_for_keepers[i]:
                costs_for_keepers[i]['yes'] = 0
            if '' not in costs_for_keepers[i]:
                costs_for_keepers[i][''] = 0
            total = float(costs_for_keepers[i]['']) + float(costs_for_keepers[i]['yes'])
            writer.writerow({'user:KEEP': costs_for_keepers[i]['user:KEEP'],
                             'non-production subtotal': costs_for_keepers[i][''],
                             'production subtotal': costs_for_keepers[i]['yes'],
                             'user total': total})

    # Overview of untagged resources
    generate_untagged_overview()


def main():
    # print_data()  # prints blob of data

    # import pdb; pdb.set_trace()
    # generate_one_report('ADAM')
    # generate_one_report('BRIAN')
    # generate_one_report('DENIS')
    generate_reports()

if __name__ == '__main__':
    SC = SpreadsheetCache()
    main()
