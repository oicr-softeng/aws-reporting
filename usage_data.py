__author__ = 'cleung'
# Generate reports showing AWS snapshots, AMIs, volumes, and instances; and KEEP-tags, PROD-tags
# Snapshots show associated AMIs and KEEP-tags thereof
# Volumes show associated instances and the KEEP-tags thereof
# Code borrowed heavily from Niall's previous script: volume_cleanup.py

# TODO: This works, but would be nice to have descriptions/names of associated resources as they are
# TODO: easier for humans to identify than the resource_ids
# TODO: Consider making it TSV so that we can list snapshots (or other fields) with commas and it'll still work

import os
import sys
import boto
from boto import ec2
from operator import itemgetter
import csv

# Name your output files
volumes_data_output_file = "reports/volumes.csv"
snapshots_data_output_file = "reports/snapshots.csv"
instances_data_output_file = "reports/instances.csv"
images_data_output_file = "reports/images.csv"

class Resource(object):
    def __init__(self, res_type):
        self.res_type = res_type
        self.spreadsheet = {}

        self.region_names = []
        self.get_regions()

        self.credentials = self.get_credentials()

        # populate depending on type
        if self.res_type == "instance":
            self.populate_instances()
        elif self.res_type == "snapshot":
            self.populate_snapshots()
        elif self.res_type == "volume":
            self.populate_volumes()
        elif self.res_type == "image":
            self.populate_images()

    def get_credentials(self):
        return {"aws_access_key_id": os.environ['AWS_ACCESS_KEY'],
                "aws_secret_access_key": os.environ['AWS_SECRET_KEY']}

    def get_regions(self):
        regions = ec2.regions()
        for region in regions:
            self.region_names.append(region.name)

    def get_volumes(self, region):
        """Return list of whole volumes for a given region"""
        try:
            conn = ec2.connect_to_region(region, **self.credentials)
            region_volumes = conn.get_all_volumes()
        except boto.exception.EC2ResponseError:
            return []  # This better not fail silently or I'll cut a person.
        return region_volumes

    def get_all_volumes(self):
        all_volumes = []
        for region in self.region_names:
            all_volumes.extend(self.get_volumes(region))
        return all_volumes

    def get_instances(self, region):
        """Return list of whole instances for given region"""
        try:
            conn = ec2.connect_to_region(region, **self.credentials)
            region_instances = []
            reservations = conn.get_all_reservations()
            for reservation in reservations:
                for instance in reservation.instances:
                    region_instances.append(instance)
        except boto.exception.EC2ResponseError:
            return []
        return region_instances

    def get_all_instances(self):
        all_instances = []
        for region in self.region_names:
            all_instances.extend(self.get_instances(region))
        return all_instances

    def get_snapshots(self, region):
        """Return list of whole snapshots for a given region"""
        try:
            conn = ec2.connect_to_region(region, **self.credentials)
            region_snapshots = conn.get_all_snapshots(owner='self')
        except boto.exception.EC2ResponseError:
            return []
        return region_snapshots

    def get_all_snapshots(self):
        all_snapshots = []
        for region in self.region_names:
            all_snapshots.extend(self.get_snapshots(region))
        return all_snapshots

    @staticmethod
    def get_name_tag(obj):
        """ 'Name' is an optional tag. Get it if it exists."""
        if 'Name' in obj.tags:
            return obj.tags['Name']
        else:
            return ""

    @staticmethod
    def get_keep_tag(obj):
        """Get the KEEP tag from source, if it exists. Empty strings count as untagged in this version."""
        if 'KEEP' in obj.tags and len(obj.tags['KEEP'].strip()) != 0:
            return obj.tags['KEEP']
        else:
            return "-------no-tag"

    @staticmethod
    def is_production(obj):
        return 'PROD' in obj.tags

    @staticmethod
    def get_amis_of(snapshot_id):
        """Get the AMI ids associated with a given snapshot"""
        mes_amis = []
        # There has GOT to be a better way. Hmm... maybe not
        keys = Ims.spreadsheet.keys()
        for key in keys:
            if snapshot_id in Ims.spreadsheet[key]['associated_snapshots']:
                mes_amis.append(key)
        return mes_amis

    @staticmethod
    def get_snapshots_of(image):
        """Return the snapshot ids (strings) associated with this AMI"""
        snapshot_ids = []
        device_mapping = image.block_device_mapping  # dict of devices
        devices = device_mapping.keys()
        for device in devices:
            if device_mapping[device].snapshot_id is not None:
                snapshot_ids.append(device_mapping[device].snapshot_id.encode())  # do I need to have 'encode' here?
        return snapshot_ids

    def get_images(self, region):
        """Get whole AMIs for a given region"""
        try:
            conn = ec2.connect_to_region(region, **self.credentials)
            region_images = conn.get_all_images(owners=['self'])
        except boto.exception.EC2ResponseError:
            return []
        return region_images

    def get_all_images(self):
        all_images = []
        for region in self.region_names:
            all_images.extend(self.get_images(region))
        return all_images

    def populate_images(self):
        """Dict of dicts for images"""
        print "Populating images info..."
        images = self.get_all_images()
        for i in images:

            associated_snapshots = self.get_snapshots_of(i)

            self.spreadsheet[i.id] = dict(name=i.name, Name_tag=self.get_name_tag(i), id=i.id,
                                          KEEP_tag=self.get_keep_tag(i), PROD_tag=self.is_production(i),
                                          region=i.region.name,
                                          created=i.creationDate,
                                          associated_snapshots=associated_snapshots,
                                          description=i.description)

    def populate_volumes(self):
        """Dictionary of dictionaries representing volumes"""
        print "Populating volumes info..."
        volumes = self.get_all_volumes()
        for i in volumes:

            # handle associated instance's KEEP-tag
            associated_instance_id = i.attach_data.instance_id

            if associated_instance_id is None:  # sometimes there is no attached instance
                instance_keep_tag = "-------no-instance-found"
            else:
                instance_keep_tag = Ins.spreadsheet[associated_instance_id]['KEEP_tag']
            self.spreadsheet[i.id] = dict(Name_tag=self.get_name_tag(i), id=i.id, KEEP_tag=self.get_keep_tag(i),
                                          instance_KEEP_tag=instance_keep_tag,
                                          associated_instance_id=associated_instance_id,
                                          PROD_tag=self.is_production(i), attachment_state=i.attachment_state(),
                                          state=i.volume_state(), status=i.status, iops=i.iops, size=i.size,
                                          created=i.create_time, region=i.region.name)

    def populate_instances(self):
        """Make a dictionary of dictionaries with the all fields we want
        Dict is nice so that we can easily look up instance KEEP-tags later.
        """
        print "Populating instances info..."
        instances = self.get_all_instances()
        for i in instances:
            self.spreadsheet[i.id] = dict(Name_tag=self.get_name_tag(i), id=i.id, KEEP_tag=self.get_keep_tag(i),
                                          PROD_tag=self.is_production(i), instance_type=i.instance_type,
                                          state=i.state, launched=i.launch_time, region=i.region.name)

    def populate_snapshots(self):
        """Dict of dicts for snapshots"""
        print "Populating snapshots info..."
        snapshots = self.get_all_snapshots()

        for i in snapshots:

            # find the ami id(s) for this snapshot. API allows for multiple even though I don't think there would be
            associated_ami_ids = self.get_amis_of(i.id)

            ami_keep_tags = [Ims.spreadsheet[ami_id]['KEEP_tag'] for ami_id in associated_ami_ids]

            self.spreadsheet[i.id] = dict(Name_tag=self.get_name_tag(i), id=i.id, KEEP_tag=self.get_keep_tag(i),
                                          ami_KEEP_tag=ami_keep_tags, associated_ami_ids=associated_ami_ids,
                                          PROD_tag=self.is_production(i), start_time=i.start_time,
                                          region=i.region.name, associated_volume=i.volume_id,
                                          volume_size=i.volume_size, description=i.description)

def generate_volumes_report():
    # sort it, well, this is messy, do I have to turn it into a list? Seems like it.
    list_volumes = sorted(Vols.spreadsheet.values(), key=itemgetter('instance_KEEP_tag', 'KEEP_tag', 'region',
                                                                    'created'))

    # dump it to see what it looks like
    print "Writing to file..."
    with open(volumes_data_output_file, 'w') as f:
        fields = ['Name', 'volume id', 'volume KEEP tag', 'instance KEEP tag', 'associated instance id', 'production?',
                  'attachment state', 'volume state', 'status', 'iops', 'size', 'created', 'region']
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in list_volumes:
            writer.writerow({'Name': row['Name_tag'], 'volume id': row['id'], 'volume KEEP tag': row['KEEP_tag'],
                             'instance KEEP tag': row['instance_KEEP_tag'],
                             'associated instance id': row['associated_instance_id'], 'production?': row['PROD_tag'],
                             'attachment state': row['attachment_state'], 'volume state': row['state'],
                             'status': row['status'], 'iops': row['iops'], 'size': row['size'],
                             'created': row['created'], 'region': row['region']})

def generate_snapshots_report():
    list_snapshots = sorted(Snaps.spreadsheet.values(), key=itemgetter('ami_KEEP_tag', 'KEEP_tag', 'region',
                                                                       'start_time'))

    # dump it to see what it looks like
    print "Writing to file " + snapshots_data_output_file + "..."
    with open(snapshots_data_output_file, 'w') as f:
        fields = ['Name', 'snapshot id', 'snapshot KEEP tag', 'AMI KEEP tag', 'associated AMI id', 'production?',
                  'start time', 'region', 'associated volume', 'volume size', 'description']
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in list_snapshots:

            # deal with none, single, or multi AMIs and their respective KEEP-tags, if existent
            associated_ami_ids = ""
            ami_keep_tags = ""
            if len(row['associated_ami_ids']) == 1:
                associated_ami_ids = row['associated_ami_ids'][0]
                ami_keep_tags = row['ami_KEEP_tag'][0]
            elif len(row['associated_ami_ids']) == 0:
                associated_ami_ids = "-------no-AMI-found"
                ami_keep_tags = "-------no-AMI-found"
            else:
                for ami_ids in row['associated_ami_ids']:
                    associated_ami_ids += ami_ids + " "
                for keeps in row['ami_KEEP_tag']:
                    ami_keep_tags += keeps + " "

            writer.writerow({'Name': row['Name_tag'], 'snapshot id': row['id'], 'snapshot KEEP tag': row['KEEP_tag'],
                             'AMI KEEP tag': ami_keep_tags, 'associated AMI id': associated_ami_ids,
                             'production?': row['PROD_tag'], 'start time': row['start_time'], 'region': row['region'],
                             'associated volume': row['associated_volume'], 'volume size': row['volume_size'],
                             'description': row['description']})


def generate_instances_report():
    list_instances = sorted(Ins.spreadsheet.values(), key=itemgetter('KEEP_tag', 'region', 'launched'))

    # dump it to see what it looks like
    print "Writing to file " + instances_data_output_file + "..."
    with open(instances_data_output_file, 'w') as f:
        fields = ['Name', 'instance id', 'KEEP tag', 'production?', 'instance type', 'state', 'launched', 'region']
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in list_instances:
            writer.writerow({'Name': row['Name_tag'], 'instance id': row['id'], 'KEEP tag': row['KEEP_tag'],
                             'production?': row['PROD_tag'], 'instance type': row['instance_type'],
                             'state': row['state'], 'launched': row['launched'], 'region': row['region']})



def generate_images_report():
    list_images = sorted(Ims.spreadsheet.values(), key=itemgetter('KEEP_tag', 'region', 'created'))

    # dump it to see what it looks like
    print "Writing to file " + images_data_output_file + "..."
    with open(images_data_output_file, 'w') as f:
        fields = ['name', 'alternative name', 'image id', 'KEEP tag', 'production?', 'region', 'created',
                  'associated snapshots', 'description']
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in list_images:

            # deal with none, single, or multi AMIs and their respective KEEP-tags, if existent
            associated_snap_ids = ""
            if len(row['associated_snapshots']) == 1:
                associated_snap_ids = row['associated_snapshots'][0]
            elif len(row['associated_snapshots']) == 0:
                associated_snap_ids = "-------no-snapshots-found"
            else:
                for snap_id in row['associated_snapshots']:
                    associated_snap_ids += snap_id + " "

            writer.writerow({'name': row['name'], 'alternative name': row['Name_tag'], 'image id': row['id'],
                             'KEEP tag': row['KEEP_tag'], 'production?': row['PROD_tag'], 'region': row['region'],
                             'created': row['created'],
                             'associated snapshots': associated_snap_ids, 'description': row['description']})


def main():
    # import pdb; pdb.set_trace()
    generate_volumes_report()
    generate_snapshots_report()
    generate_instances_report()
    generate_images_report()
    print "done"

if __name__ == '__main__':
    Ins = Resource('instance')
    Vols = Resource('volume')
    Ims = Resource('image')
    Snaps = Resource('snapshot')
    main()
