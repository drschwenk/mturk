import os
import boto3
import pickle
import copy
import logging
import xmltodict
from tqdm import tqdm
import datetime
import threading
import jinja2
from botocore.exceptions import ClientError


class MturkClient:
    def __init__(self, **kwargs):
        self.in_sandbox = kwargs['in_sandbox']
        environments = {
            "live": {
                "endpoint": "https://mturk-requester.us-east-1.amazonaws.com",
                "preview": "https://www.mturk.com/mturk/preview",
                "manage": "https://requester.mturk.com/mturk/manageHITs",
                "reward": "0.00"
            },
            "sandbox": {
                "endpoint": "https://mturk-requester-sandbox.us-east-1.amazonaws.com",
                "preview": "https://workersandbox.mturk.com/mturk/preview",
                "manage": "https://requestersandbox.mturk.com/mturk/manageHITs",
                "reward": "0.01"
            },
        }

        self.mturk_environment = environments['live'] if not kwargs['in_sandbox'] else environments['sandbox']

        session = boto3.Session(profile_name=kwargs['profile_name'])
        self.client = session.client(
            service_name='mturk',
            region_name='us-east-1',
            endpoint_url=self.mturk_environment['endpoint'],
            aws_access_key_id=kwargs['aws_access_key_id'],
            aws_secret_access_key=kwargs['aws_secret_access_key']
        )
        print(self.client)

    def create_hit(self, **kwargs):
        """
        internal helper function for creating a HIT
        :param params the parameters (required and optional) common to all HITs
        :param **kwargs any other parameters needed for a specific HIT type
        :return the created HIT object
        """
        response = self.client.create_hit(**kwargs)
        return response


class MTurk:

    def __init__(self, **kwargs):
        """
        initializes the instance with AWS credentials and a host
        :param aws_access_key_id the access key id.
        :param aws_secret_access_key the secret access key.
        :param host the mturk host to connect to
        """
        # self.in_sandbox = in_sandbox
        # environments = {
        #     "live": {
        #         "endpoint": "https://mturk-requester.us-east-1.amazonaws.com",
        #         "preview": "https://www.mturk.com/mturk/preview",
        #         "manage": "https://requester.mturk.com/mturk/manageHITs",
        #         "reward": "0.00"
        #     },
        #     "sandbox": {
        #         "endpoint": "https://mturk-requester-sandbox.us-east-1.amazonaws.com",
        #         "preview": "https://workersandbox.mturk.com/mturk/preview",
        #         "manage": "https://requestersandbox.mturk.com/mturk/manageHITs",
        #         "reward": "0.01"
        #     },
        # }
        #
        # self.qualifications = {
        #     'high_accept_rate': 95,
        #     'english_speaking': ['US', 'CA', 'AU', 'NZ', 'GB'],
        #     'us_only': ['US']
        # }
        #
        # self.turk_data_schemas = {
        #     'html': 'http://mechanicalturk.amazonaws.com/AWSMechanicalTurkDataSchemas/2011-11-11/HTMLQuestion.xsd'
        # }
        #
        # self.mturk_environment = environments['live'] if not in_sandbox else environments['sandbox']
        #
        # session = boto3.Session(profile_name=profile_name)
        # self.client = session.client(
        #     service_name='mturk',
        #     region_name='us-east-1',
        #     endpoint_url=self.mturk_environment['endpoint'],
        #     aws_access_key_id=aws_access_key_id,
        #     aws_secret_access_key=aws_secret_access_key,
        # )
        self.kwargs = kwargs
        self.amt = MturkClient(**self.kwargs)
        self.n_threads = 4
        self.in_sandbox = kwargs['in_sandbox']
        self.turk_data_schemas = {
            'html': 'http://mechanicalturk.amazonaws.com/AWSMechanicalTurkDataSchemas/2011-11-11/HTMLQuestion.xsd'
        }
        self.qualifications = {
            'high_accept_rate': 95,
            'english_speaking': ['US', 'CA', 'AU', 'NZ', 'GB'],
            'us_only': ['US']
        }
        self.print_balance()


    # def run(self, hits, n_threads):
    #     hit_batches = [hits[i::n_threads] for i in range(n_threads)]
    #     for t in range(n_threads)
    #         nt = threading.Thread(target=self.expire_hits, args=(hit_batches[0], ))
    #         nt.start()
    #     t2.join()

    def get_num_balance(self):
        try:
            balance_response = self.amt.client.get_account_balance()
            return float(balance_response['AvailableBalance'])
        except ClientError as e:
            print(e)
            raise

    def print_balance(self):
        balance = self.get_num_balance()
        print(f'Account balance is: ${balance:.{2}f}')

    def _build_qualifications(self, locales=None):
        masters_id = '2ARFPLSP75KLA8M8DH1HTEQVJT3SY6' if self.in_sandbox else '2F1QJWKUDD8XADTFD2Q0G6UTO95ALH'
        master = {
            'QualificationTypeId': masters_id,
            'Comparator': 'EqualTo',
            'RequiredToPreview': True,
        }
        high_accept_rate = {
            'QualificationTypeId': '000000000000000000L0',
            'Comparator': 'GreaterThanOrEqualTo',
            'IntegerValues': [self.qualifications['high_accept_rate']],
            'RequiredToPreview': True,
        }
        location_based = {
            'QualificationTypeId': '00000000000000000071',
            'Comparator': 'in',
            'LocaleValue': locales,
            'RequiredToPreview': True,
        }
        return [high_accept_rate]



    @classmethod
    def _render_hit_html(cls, template_params, **kwargs):
        env = jinja2.Environment(loader=jinja2.FileSystemLoader(template_params['template_dir']))
        template = env.get_template(template_params['template_file'])
        hit_html = template.render(**kwargs)
        return hit_html

    def preview_hit_interface(self, template_params, **kwargs):
        hit_html = self._render_hit_html(template_params, **kwargs)
        html_dir = './html_renders'
        html_out_file = os.path.join(html_dir, 'task_preview.html')
        if not os.path.exists(html_dir):
            os.makedirs(html_dir)
        with open(html_out_file, 'w') as f:
            f.write(hit_html)

    def _create_question_xml(self, html_question, frame_height, turk_schema='html'):
        hit_xml = f"""\
            <HTMLQuestion xmlns="{self.turk_data_schemas[turk_schema]}">
                <HTMLContent><![CDATA[
                    <!DOCTYPE html>
                        {html_question}
                    ]]>
                </HTMLContent>
                <FrameHeight>{frame_height}</FrameHeight>
            </HTMLQuestion>"""
        try:
            xmltodict.parse(hit_xml)
            return hit_xml
        except xmltodict.expat.ExpatError as e:
            print(e)
            raise

    def create_html_hit_params(self, basic_hit_params, template_params, **kwargs):
        """
        creates a HIT for a question with the specified HTML
        # :param params a dict of the HIT parameters, must contain a "html" parameter
        # :return the created HIT object
        """
        hit_params = copy.deepcopy(basic_hit_params)
        frame_height = hit_params.pop('frame_height')
        question_html = self._render_hit_html(template_params, **kwargs)
        hit_params['Question'] = self._create_question_xml(question_html, frame_height)
        hit_params['QualificationRequirements'] = self._build_qualifications()
        return hit_params

    def create_hit_group(self, data, task_param_generator, **kwargs):
        hit_params = [self.create_html_hit_params(**kwargs, **task_param_generator(point)) for point in data]
        hit_batches = [hit_params[i::self.n_threads] for i in range(self.n_threads)]
        print(len(hit_batches))
        threads = []
        for batch in hit_batches:
            t = CreateHits(batch, **self.kwargs)
            threads.append(t)

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

    def get_all_hits(self):
        paginator = self.amt.client.get_paginator('list_hits')
        response_iterator = paginator.paginate(
            PaginationConfig={
                'PageSize': 100,
            }
        )
        response = []
        for r in response_iterator:
            response.extend(r['HITs'])
        return response

    def expire_hits(self, hits):
        hit_batches = [hits[i::self.n_threads] for i in range(self.n_threads)]
        threads = []
        for batch in hit_batches:
            t = ExpireHits(batch, **self.kwargs)
            threads.append(t)
            t.start()

    def delete_hits(self, hits):
        responses = [self.amt.client.delete_hit(HITId=h['HITId']) for h in tqdm(hits) if h['HITStatus'] != 'Disposed']

    def force_delete_hits(self, hits):
        self.expire_hits(hits)
        self.delete_hits(hits)

    def set_hits_reviewing(self, hits):
        responses = [self.client.update_hit_review_status(HITId=h['HITId'], Revert=False) for h in hits]

    def revert_hits_reviewable(self, hits):
        responses = [self.client.update_hit_review_status(HITId=h['HITId'], Revert=True) for h in hits]

    def get_all_assignments(self, hits=()):
        assignments = []
        if not hits:
            hits = self.get_all_hits()
        for hit in hits:
            assignments.append(self.client.list_assignments_for_hit(
                HITId=hit['HITId'],
                AssignmentStatuses=['Submitted', 'Approved'],
                MaxResults=10)
            )
        return assignments

    def approve_assignments(self, assignments):
        for hit in assignments:
            for assignment in hit['Assignments']:
                if assignment['AssignmentStatus'] == 'Submitted':
                    assignment_id = assignment['AssignmentId']
                    print('Approving Assignment {}'.format(assignment_id))
                    self.client.approve_assignment(
                        AssignmentId=assignment_id,
                        RequesterFeedback='good',
                        OverrideRejection=False,
                    )


class BotoThreadedOperation(threading.Thread):

    def __init__(self, **kwargs):
        self.amt = MturkClient(**kwargs)
        super().__init__()


class ExpireHits(BotoThreadedOperation):
    def __init__(self, hits, **kwargs):
        super().__init__(**kwargs)
        self.hits = hits
        self.exp_date = datetime.datetime(2001, 1, 1)

    def run(self):
        responses = [self.amt.client.update_expiration_for_hit(HITId=h['HITId'], ExpireAt=self.exp_date)
                     for h in tqdm(self.hits)]


class CreateHits(BotoThreadedOperation):
    def __init__(self, batch, **kwargs):
        super().__init__(**kwargs)
        self.batch = batch

    def run(self):
        responses = [self.amt.create_hit(**point) for point in self.batch]
        return responses


class HITGroup:
    pass

