# scratch attempt at more general mturk api wrapper

# from boto.mturk.connection import MTurkConnection
# from boto.mturk.price import Price
# from boto.mturk.question import HTMLQuestion, ExternalQuestion
# import pickle
import numpy as np

SANDBOX_HOST = "mechanicalturk.sandbox.amazonaws.com"
PROD_HOST = HOST = 'mechanicalturk.amazonaws.com'


def pickle_this(results, file_name):
    with open(file_name, 'wb') as f:
        pickle.dump(results, f, protocol=2)


def unpickle_this(file_name):
    with open(file_name, 'rb') as f:
        results_df = pickle.load(f)
    return results_df


def expected_cost(hit_group, static_params, amt_con=None):
    import warnings

    def custom_formatwarning(msg, *a):
        return str(msg) + '\n'

    cost = len(hit_group) * static_params['amount'] * static_params['max_assignments']

    warnings.formatwarning = custom_formatwarning
    if amt_con:
        account_balance = amt_con.get_account_balance().amount
        if account_balance < cost:
            warnings.warn('Insufficient Funds')
    return round(cost, 2)


def annotation_filter(annotations, hit):
    """
    checks whether a HIT matches a set of annotations. if the HIT has no annotation
    it will have no .RequesterAnnotation property, hence the try/except
    """
    # If no annotations specified, all HITs pass the filter.
    if annotations is None:
        return True
    # Otherwise, try to check the annotation.
    try:
        return hit.RequesterAnnotation in annotations
    # AttributeError means HIT has no .RequesterAnnotation property, so it
    # fails to match the filter
    except AttributeError:
        return False


class MTurk(object):
    """
    A class that wraps a boto.mturk.connection object and provides methods for
    the most common AI2 use cases
    """

    def __init__(self, aws_access_key_id, aws_secret_access_key, host=SANDBOX_HOST):
        """
        initializes the instance with AWS credentials and a host
        :param aws_access_key_id the access key id.
        :param aws_secret_access_key the secret access key.
        :param host the mturk host to connect to
        """
        self.connection = MTurkConnection(aws_access_key_id=aws_access_key_id,
                                          aws_secret_access_key=aws_secret_access_key,
                                          host=host)
        self.host = host

    def __del__(self):
        """
        close the connection whenever this object goes out of scope
        """
        self.connection.close()

    def get_account_balance(self):
        """
        :return the balance on the mturk account
        """
        return self.connection.get_account_balance()[0]

    def _create_hit(self, params, **kwargs):
        """
        internal helper function for creating a HIT
        :param params the parameters (required and optional) common to all HITs
        :param **kwargs any other parameters needed for a specific HIT type
        :return the created HIT object
        """
        return self.connection.create_hit(
            title=params["title"],
            description=params["description"],
            keywords=params["keywords"],
            max_assignments=params["max_assignments"],
            reward=Price(amount=params["amount"]),
            qualifications=params["qualifications"],
            lifetime=params["lifetime"],
            # optional params below
            annotation=params.get("annotation"),
            **kwargs
        )

    def create_url_hit(self, params):
        """
        creates a HIT for an external question with a specified URL
        :param params a dict of the HIT parameters. must contain a "url" parameter
        :return the created HIT object
        """
        question = ExternalQuestion(params["url"], params["frame_height"])
        return self._create_hit(params, question=question)

    def create_html_hit(self, params):
        """
        creates a HIT for a question with the specified HTML
        :param params a dict of the HIT parameters, must contain a "html" parameter
        :return the created HIT object
        """
        question = HTMLQuestion(params["html"], params["frame_height"])
        return self._create_hit(params, question=question)

    def create_layout_hit(self, params):
        """
        creates a HIT for a question using the supplied layout id
        :param params a dict of the HIT parameters, must contain a "hit_layout"
               parameters with the layout id, and a "layout_params" parameter
               that's the dict of parameters to feed to the layout.
        """
        # create the LayoutParameters object from the supplied params
        layout_params = LayoutParameters([
            LayoutParameter(name, value)
            for name, value in params["layout_params"]
        ])

        return self._create_hit(params,
                                hit_layout=params["hit_layout"],
                                layout_params=layout_params)
    
    def delete_all_hits(self):
        """
        Permanently disables/ deletes all of the user's active HITs.
        :param mturk_connection: active mturk connection established by user in the notebook.
        :return:
        """
        my_hits = list(self.get_all_hits())
        for hit in my_hits:
            self.connection.disable_hit(hit.HITId)

    def get_assignments_object_list(self, assignment_dict):
        """
        Returns a list of "<boto.mturk.connection.Assignment object at...>" objects
        assignment_dict: a dictionary of HITId-assignment object pairs
        """
        assignments = []
        for entry in assignment_dict:
            for assignment_object in assignment_dict[entry]:
                assignments.append(assignment_object)
        return assignments

    def get_results_dict(self, HIT_assignments):
        """
        Takes a list of HIT assignment objects as input.
        Returns a list of dictionaries of HITs containing:
        HIT_id: the HIT ID
        worker_id: the worker ID of the Turker who completed the HIT
        answers: a dictionary of qid-answer field value pairs
        """
        assignment_results = []
        for assignment in HIT_assignments:
            HIT_dict = {}
            HIT_dict["assignment_object"] = assignment
            HIT_dict["worker_Id"] = assignment.WorkerId
            HIT_dict["HIT_id"] = assignment.HITId
            answers_dict = {}
            for answer in assignment.answers[0]:
                answers_dict[answer.qid] = answer.fields
                HIT_dict["answers"] = answers_dict
            assignment_results.append(HIT_dict)
        return assignment_results

    def get_all_results(self, hits):
        all_results = {}
        for hid, assignments in self.get_assignments(hits).items():
            all_results[hid] = self.get_results_dict(assignments)
        return all_results

    def get_reviewable_hits(self, annotations=None, detailed=False):
        """
        Get all the reviewable HITs. By default returns minimal HIT objects, but
        will return detailed ones (by necessity) if annotations is specified or
        if detailed is True
        :param annotations an optional set of annotations to retrieve HITs for
        :param detailed do you want detailed HIT objects or minimal ones
        :return a list of HIT objects
        """
        minimal_hits = []
        page_num = 1
        while True:
            more_hits = self.connection.get_reviewable_hits(page_size=100,
                                                            page_number=page_num)
            if more_hits:
                minimal_hits.extend(more_hits)
                page_num += 1
            else:
                break

        if detailed or annotations is not None:
            detailed_hits = [self.connection.get_hit(hit.HITId, response_groups=('Minimal', 'HITDetail'))
                             for hit in minimal_hits]
            return [hit for hit in detailed_hits
                    if annotation_filter(annotations, hit)]
        else:
            return minimal_hits

    def get_all_hits(self, annotations=None):
        """
        Get all the HITs.
        :param annotations a set of annotations to get HITs for, all HITs if
               not specified
        :return a list of HIT objects
        """

        return [hit for hit in self.connection.get_all_hits()
                if annotation_filter(annotations, hit)]

    def get_assignments(self, hits=None, hit_ids=None, status=None):
        """
        Retrieves individual assignments associated with the supplied HITs
        :param hits the HITs to get assignments for
        :status HIT status to filter by
        :return dict from HITId to lists of assignments
        """
        if hit_ids is None:
            hit_ids = [hit.HITId for hit in hits]
        return {hit_id: self.connection.get_assignments(hit_id, status=status)
                 for hit_id in hit_ids}

    def disable_hit(self, hit=None, hit_id=None):
        """
        disable the specified hit (or the hit with the specified id). must
        specify either `hit` or `hit_id`
        :param hit a HIT object to disable
        :param hit_id a HITId to disable
        """
        hit_id = hit.HITId if hit is not None else hit_id
        return self.connection.disable_hit(hit_id)

    def approve_assignment(self, assignment=None, assignment_id=None, feedback=None):
        """
        approve the specified assignment (or the assigment with the specified id)
        must specify either `assignment` or `assignment_id`
        :param assignment an assignment object to approve
        :param assignment_id an AssignmentId to approve
        :param feedback optional feedback for the worker
        """
        assignment_id = assignment.AssignmentId if assignment is not None else assignment_id
        return self.connection.approve_assignment(assignment_id, feedback)

    def reject_assignment(self, assignment=None, assignment_id=None, feedback=None):
        """
        reject the specified assignment (or the assigment with the specified id)
        must specify either `assignment` or `assignment_id`
        :param assignment an assignment object to reject
        :param assignment_id an AssignmentId to reject
        :param feedback optional feedback for the worker
        """
        assignment_id = assignment.AssignmentId if assignment is not None else assignment_id
        return self.connection.reject_assignment(assignment_id, feedback)


class HITDataBatch(object):

    def __init__(self):
        pass

        self.data = None
        self.metadata = None

    def save(self):
        pass

    def load(self):
        pass


