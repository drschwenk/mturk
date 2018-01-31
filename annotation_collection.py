import pickle
from collections import defaultdict
from jinja2 import Environment, FileSystemLoader
import os
import json
from nltk.tokenize import sent_tokenize
import PIL.Image as Image
import requests


from boto.mturk.qualification import PercentAssignmentsApprovedRequirement, Qualifications, Requirement, LocaleRequirement


def create_result(assmt):
    result = json.loads(assmt.answers[0][0].fields[0])
    result['h_id'] = assmt.HITId
    return result


def pickle_this(results_df, file_name):
    with open(file_name, 'w') as f:
        pickle.dump(results_df, f)


def un_pickle_this(file_name):
    with open(file_name, 'r') as f:
        results_df = pickle.load(f)
    return results_df


def write_task_page(page_html):
    html_dir = './html_renders'
    html_out_file = os.path.join(html_dir, 'img_rate.html')
    if not os.path.exists(html_dir):
        os.makedirs(html_dir)
    with open(html_out_file, 'w') as f:
        f.write(page_html)


def generate_task_page(s3_base_path, img_id, template_file='character_bbox.html'):
    env = Environment(loader=FileSystemLoader('hit_templates'))
    template = env.get_template(template_file)
    page_html = template.render(s3_uri_base=s3_base_path, image_id=img_id)
    return page_html


def generate_simpler_task_page(s3_base_path, img_id, n_chars, template_file='character_bbox_simple.html'):
    pages = []
    for char_idx in range(n_chars):
        env = Environment(loader=FileSystemLoader('hit_templates'))
        template = env.get_template(template_file)
        char_img = img_id.rsplit('_', 1)[0] + '_char_' + str(char_idx) + '_taskb.png'
        page_html = template.render(s3_uri_base=s3_base_path, image_id=img_id, char_img=char_img)
        page_html = page_html
        pages.append(page_html)
    return pages


def generate_stage_4a_task_page(img_id, formatted_description, template_file='stage_4a.html'):
    env = Environment(loader=FileSystemLoader('hit_templates'))
    template = env.get_template(template_file)
    page_html = template.render(image_id=img_id, formatted_description=formatted_description)
    page_html = page_html
    return page_html


def generate_stage_4b_task_page(img_id, formatted_description, target, template_file='stage_4b.html'):
    env = Environment(loader=FileSystemLoader('hit_templates'))
    template = env.get_template(template_file)
    page_html = template.render(s3_uri_base=s3_subtask_path, image_id=img_id, description=formatted_description, target_object=target)
    return page_html


def generate_segm_anno_task_page(ent, s3_base, template_file='img_seg.html'):
    img_id = ent.gid() + '_bb.png'
    ent_label = ent.data()['entityLabel']
    env = Environment(loader=FileSystemLoader('hit_templates'))
    template = env.get_template(template_file)
    page_html = template.render(base_url=s3_base, image_name=img_id, entity_label=ent_label)
    return page_html


def generate_stage_4_task_page(s3_base_path, img_id, n_chars, template_file='stage_4.html'):
    pages = []
    for char_idx in range(n_chars):
        env = Environment(loader=FileSystemLoader('hit_templates'))
        template = env.get_template(template_file)
        char_img = img_id.rsplit('_', 1)[0] + '_char_' + str(char_idx) + '_taskb.png'
        page_html = template.render(s3_uri_base=s3_base_path, image_id=img_id, char_img=char_img)
        page_html = page_html
        pages.append(page_html)
    return pages


def generate_simpler_supl_task_page(s3_base_path, img_id, char_id, template_file='character_bbox_simple.html'):
    env = Environment(loader=FileSystemLoader('hit_templates'))
    template = env.get_template(template_file)
    char_img = char_id
    img_id = img_id + '_taskb.png'
    page_html = template.render(s3_uri_base=s3_base_path, image_id=img_id, char_img=char_img)
    return page_html


def filter_hits_by_date(hit_group, start_date, end_date):
    import pytz
    import dateutil.parser as dt_parse
    import datetime

    start_datetime = datetime.datetime(*start_date).replace(tzinfo=pytz.UTC)
    end_datetime = datetime.datetime(*end_date).replace(tzinfo=pytz.UTC)
    return [hit for hit in hit_group if start_datetime < dt_parse.parse(hit.CreationTime) < end_datetime]


def filter_hits_by_date_old(hit_group, day_of_month, hour=None):
    import dateutil.parser as dt_parse

    def check_day(hit, day_of_month):
        return day_of_month == dt_parse.parse(hit.CreationTime).day

    def check_hour(hit, hour):
        return hour == dt_parse.parse(hit.CreationTime).hour

    filtered_hits = [hit for hit in hit_group if check_day(hit, day_of_month)]
    if hour:
        filtered_hits = [hit for hit in filtered_hits if check_hour(hit, hour)]
    return filtered_hits


def filter_hits_by_completion(hit_group, n_assigments=3):
    return [hit for hit in hit_group if int(hit.NumberOfAssignmentsCompleted) == n_assigments]


def filter_hits_by_status(hit_group, status='Reviewable'):
    return [hit for hit in hit_group if hit.HITStatus == status]


def get_completed_hits(mturk_connection):
    """
    Queries amt for all active user HITs.
    :param mturk_connection: active mturk connection established by user in the nb.
    :return: list of boto HIT result objects
    """
    reviewable_hits = []
    page_n = 1
    hits_left = True
    while hits_left:
        hit_range = mturk_connection.get_reviewable_hits(page_size=100, page_number=page_n)
        if not hit_range:
            hits_left = False
            break
        reviewable_hits.extend(hit_range)
        page_n += 1
    return reviewable_hits


def get_assignments(mturk_connection, reviewable_hits, status=None):
    """
    Retrieves individual assignments associated with the specified HITs.
    :param mturk_connection: active mturk connection established by user in the nb.
    :param reviewable_hits: HITs to review
    :param status: HIT status to filter by.
    :return: hit_id:assignment dict
    """
    assignments = defaultdict(list)
    for hit in reviewable_hits:
        assignment = mturk_connection.get_assignments(hit.HITId, status=status)
        assignments[hit.HITId].extend(assignment)
    return assignments


def build_hit_params(qhtml, static_params):
    """
    Dynamically builds some HIT params that will change based on the book/url
    :param url: formatted url of page image on s3
    :param static_params: Universal HIT params (set by user in notebook).
    :return: complete HIT parameters.
    """
    import copy
    import boto

    def build_qualifications(locales=None):
        """
        Creates a single qualification that workers have a > 95% acceptance rate.
        :return: boto qualification obj.
        """
        qualifications = Qualifications()
        requirements = [PercentAssignmentsApprovedRequirement(comparator="GreaterThan", integer_value="95")]
        if locales:
            loc_req = LocaleRequirement(
                    comparator='In',
                    locale=locales)
            requirements.append(loc_req)
        _ = [qualifications.add(req) for req in requirements]
        return qualifications
    if 'locales' not in static_params:
        static_params['locales'] = None
    hit_params = copy.deepcopy(static_params)
    hit_params['qualifications'] = build_qualifications(static_params['locales'])
    hit_params['reward'] = boto.mturk.price.Price(hit_params['amount'])
    hit_params['html'] = qhtml
    return hit_params


def prepare_simpler_hit(s3_base_path, still_id, n_chars, static_parameters):
    question_html = generate_simpler_task_page(s3_base_path, still_id, n_chars)
    return [build_hit_params(qhtml, static_parameters) for qhtml in question_html]


def prepare_simpler_supl_hit(s3_base_path, still_id, char_id, static_parameters):
    question_html = generate_simpler_supl_task_page(s3_base_path, still_id, char_id)
    return build_hit_params(question_html, static_parameters)


def prepare_stage_1b(s3_base_path, vid, static_parameters):
    still_id = vid.gid()
    question_htmls = [generate_simpler_supl_task_page(s3_base_path, still_id, char.gid()) for char in vid.data()['characters']]
    return [build_hit_params(qhtml, static_parameters) for qhtml in question_htmls]


def prepare_hit(s3_base_path, img_uri, static_parameters, task_generator=generate_task_page):
    question_html = task_generator(s3_base_path, img_uri)
    return build_hit_params(question_html, static_parameters)


def generate_stage_2_task_page(s3_base_paths, vid_anno, poses, position_prepositions, template_file='stage_2a.html'):
    pages = []
    for char in vid_anno['characters']:
        env = Environment(loader=FileSystemLoader('hit_templates'))
        template = env.get_template(template_file)
        image_url = s3_base_paths['stills'] + vid_anno['keyFrames'][0].replace('_40.png', '_10.png')
        char_url = s3_base_paths['subtask'] + char['imageID']
        page_html = template.render(s3_uri_base=s3_base_path, image_url=image_url, char_img=char_url, pose_select=poses,
                                    position_select=position_prepositions)
        page_html = page_html
        pages.append(page_html)
    return pages


def generate_stage_2b_task_page(s3_base_paths, vid_anno, template_file='stage_2b.html'):
    pages = []
    for char in vid_anno['characters']:
        env = Environment(loader=FileSystemLoader('hit_templates'))
        template = env.get_template(template_file)
        image_url = s3_base_paths['gifs'] + vid_anno['globalID'] + '.gif'
        char_url = s3_base_paths['subtask'] + char['imageID']
        page_html = template.render(s3_uri_base=s3_base_path, image_url=image_url, char_img=char_url)
        page_html = page_html
        pages.append(page_html)
    return pages


def generate_stage_3b_task_page(s3_base_paths, vid_anno, template_file='stage_3b.html'):
    env = Environment(loader=FileSystemLoader('hit_templates'))
    template = env.get_template(template_file)
    vid_setting = vid_anno.setting()
    image_url = s3_base_paths['gifs'] + vid_anno.gid() + '.gif'
    char_tuples = []
    strings_to_match = []
    for char in vid_anno.data()['characters']:
        char_name = char.data()['entityLabel']
        strings_to_match.append(char_name)
        char_url = s3_base_paths['subtask'] + char.gid() + '_taskb.png'
        char_tuples.append((char_url, char_name))
    strings_to_match.append(vid_setting)
    strings_to_match = 'string_join_token'.join(strings_to_match)
    page_html = template.render(s3_uri_base=s3_base_path, image_url=image_url, char_images=char_tuples,
                                setting=vid_setting, strings_to_match=strings_to_match)
    return page_html


def generate_baseline_a_task_page(s3_base_paths, vid_anno, match_anno, template_file='baseline_a.html'):
    env = Environment(loader=FileSystemLoader('hit_templates'))
    template = env.get_template(template_file)
    vid_setting = vid_anno.setting()
    image_url = s3_base_paths['gifs'] + vid_anno.gid() + '.gif'
    match_url = s3_base_paths['gifs'] + match_anno.gid() + '.gif'
    char_tuples = []
    strings_to_match = []
    for char in vid_anno.data()['characters']:
        char_name = char.data()['entityLabel']
        strings_to_match.append(char_name)
        char_url = s3_base_paths['subtask'] + char.gid() + '_taskb.png'
        char_tuples.append((char_url, char_name))
    strings_to_match.append(vid_setting)
    strings_to_match = 'string_join_token'.join(strings_to_match)
    page_html = template.render(s3_uri_base=s3_base_path, image_url=image_url, match_url=match_url,
                                char_images=char_tuples, setting=vid_setting, strings_to_match=strings_to_match)
    return page_html


def generate_stage_3_task_page(s3_base_paths, vid, template_file='stage_3a.html'):
    env = Environment(loader=FileSystemLoader('hit_templates'))
    template = env.get_template(template_file)
    image_url = s3_base_paths['gifs'] + vid.gid() + '.gif'
    page_html = template.render(s3_uri_base=s3_base_path, image_url=image_url)
    page_html = page_html
    return page_html


def prepare_stage_2_hit(s3_base_path, img_uri, poses, position_prepositions, static_parameters, task_generator=generate_stage_2_task_page):
    question_html = task_generator(s3_base_path, img_uri, poses, position_prepositions)
    return [build_hit_params(qhtml, static_parameters) for qhtml in question_html]


def prepare_stage_2b_hit(s3_base_path, img_uri, static_parameters, task_generator=generate_stage_2b_task_page):
    question_html = task_generator(s3_base_path, img_uri)
    return [build_hit_params(qhtml, static_parameters) for qhtml in question_html]


def prepare_stage_3_hit(s3_base_path, img_uri, static_parameters, task_generator=generate_stage_3_task_page):
    question_html = task_generator(s3_base_path, img_uri)
    return build_hit_params(question_html, static_parameters)


def prepare_stage_3b_hit(s3_base_path, img_uri, static_parameters, task_generator=generate_stage_3b_task_page):
    question_html = task_generator(s3_base_path, img_uri)
    return build_hit_params(question_html, static_parameters)


def prepare_baseline_a_hit(s3_base_path, img_uri, match_uri, static_parameters, task_generator=generate_baseline_a_task_page):
    question_html = task_generator(s3_base_path, img_uri, match_uri)
    return build_hit_params(question_html, static_parameters)


def prepare_stage_4_hit(s3_base_path, still_id, n_objs, static_parameters):
    question_html = generate_stage_4_task_page(s3_base_path, still_id, n_objs)
    return [build_hit_params(qhtml, static_parameters) for qhtml in question_html]


def prepare_stage_4a_hit(still_id, description, static_parameters):
    formatted_desc = [[word.encode('utf8') for word in sent.split()] for sent in description.split('.')][:-1]

    question_html = generate_stage_4a_task_page(still_id, formatted_desc)
    return build_hit_params(question_html, static_parameters)


def prepare_stage_4b_hit(video, static_parameters):
    still_id = video.gid()
    description = video.description()
    objects = video._data['objects']
    question_html = []
    for obj in objects:
        target_obj = obj.data()['localID']
        target_span = obj.data()['labelSpan']
        question_html.append(generate_stage_4b_task_page(still_id,
                                                         rejoin_formatted_desc(description, target_span), target_obj))
    return [build_hit_params(qhtml, static_parameters) for qhtml in question_html]


def prepare_segm_anno_hit(video, s3_base_path, static_parameters):
    entities = video._data['objects'] + video._data['characters']
    hit_html = [generate_segm_anno_task_page(ent, s3_base_path) for ent in entities]
    return [build_hit_params(hhtml, static_parameters) for hhtml in hit_html]


def rejoin_formatted_desc(description, replacement_span):
    try:
        tokenized_description = [sent.split() for sent in sent_tokenize(description)]
        replace_word = tokenized_description[replacement_span[0]][replacement_span[1]]
    except IndexError:
        tokenized_description = [sent.split() for sent in description.split('.')]
        replace_word = tokenized_description[replacement_span[0]][replacement_span[1]]
    tokenized_description[replacement_span[0]][replacement_span[1]] = '<target>' + replace_word + '</target>'
    joined_desc = ' '.join([' '.join([w for w in sent]) for sent in tokenized_description])
    return joined_desc


s3_base_path = 'https://s3-us-west-2.amazonaws.com/ai2-vision-animation-gan/annotation_data/still_frames/'
s3_subtask_path = 'https://s3-us-west-2.amazonaws.com/ai2-vision-animation-gan/annotation_data/subtask_frames/'


def display_image(still_id):
    image_url = s3_base_path + still_id
    return Image.open(requests.get(image_url, stream=True).raw)