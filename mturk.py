import os
import boto3
import pickle
import logging
from botocore.exceptions import ClientError


class MTurk(object):

    def __init__(self, aws_access_key_id, aws_secret_access_key, profile_name=None, in_sandbox=True):
        """
        initializes the instance with AWS credentials and a host
        :param aws_access_key_id the access key id.
        :param aws_secret_access_key the secret access key.
        :param host the mturk host to connect to
        """
        self.in_sandbox = in_sandbox
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

        self.qualifications = {
            'high_accept_rate': 95,
            'english_speaking': ['US', 'CA', 'AU', 'NZ', 'GB'],
            'us_only': ['US']
        }

        self.mturk_environment = environments['live'] if not in_sandbox else environments['sandbox']

        session = boto3.Session(profile_name=profile_name)
        self.client = session.client(
            service_name='mturk',
            region_name='us-east-1',
            endpoint_url=self.mturk_environment['endpoint'],
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )
        self.print_balance()

    def get_num_balance(self):
        try:
            balance_response = self.client.get_account_balance()
            return float(balance_response['AvailableBalance'])
        except ClientError as e:
            print(e)
            raise

    def print_balance(self):
        balance = self.get_num_balance()
        print(f'Account balance is: ${balance:.{2}f}')

    # def build_qualifications(locales=None):
    #     """
    #     Creates a single qualification that workers have a > 95% acceptance rate.
    #     :return: boto qualification obj.
    #     """
    #     qualifications = Qualifications()
    #     requirements = [PercentAssignmentsApprovedRequirement(comparator="GreaterThan", integer_value="95")]
    #     if locales:
    #         loc_req = LocaleRequirement(
    #             comparator='In',
    #             locale=locales)
    #         requirements.append(loc_req)
    #     _ = [qualifications.add(req) for req in requirements]
    #     return qualifications

    def build_qualifications(self, locales=None):
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

    def _create_hit(self, **kwargs):
        """
        internal helper function for creating a HIT
        :param params the parameters (required and optional) common to all HITs
        :param **kwargs any other parameters needed for a specific HIT type
        :return the created HIT object
        """
        response = self.client.create_hit(
            # MaxAssignments=params['max_assignments'],
            # LifetimeInSeconds=params['lifetime'],
            # AssignmentDurationInSeconds=600,
            # Reward=self.mturk_environment['reward'],
            # Title='Answer a simple question',
            # Keywords=params['keywords'],
            # Description=params['description'],
            **kwargs
        )
        return response

    def create_html_hit(self, hit_params, question_html):
        """
        creates a HIT for a question with the specified HTML
        :param params a dict of the HIT parameters, must contain a "html" parameter
        :return the created HIT object
        """

        hit_params['QualificationRequirements'] = self.build_qualifications()
        hit_params['Question'] = self.create_question_xml(question_html)
        return self._create_hit(**hit_params)

    @staticmethod
    def create_question_xml(html_question):
        return f"""<HTMLQuestion xmlns="http://mechanicalturk.amazonaws.com/AWSMechanicalTurkDataSchemas/2011-11-11/HTMLQuestion.xsd">
            <HTMLContent><![CDATA[
            <!DOCTYPE html>
                {html_question}
            ]]>
              </HTMLContent>
              <FrameHeight>450</FrameHeight>
            </HTMLQuestion>"""

