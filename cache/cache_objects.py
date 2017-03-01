from queue import PriorityQueue


class Score:

    def __init__(self, score, video, cache):
        self.score = score
        self.video = video
        self.cache = cache

    def __lt__(self, other):
        return self.video.size < other.video.size


class Request:

    def __init__(self, required_video = None):
        self.request_times = 0
        self.requiring_ep = []
        self.required_video = required_video

    def __lt__(self, other):
        return self.required_video < other.required_video

    def add_ep(self, ep, times):
        self.requiring_ep.append(ep)
        self.request_times += times


class Video:

    def __init__(self, size, video_id, end_point_id):
        self.size = size
        self.video_id = video_id
        self.end_point_id = end_point_id

    def __lt__(self, other):
        return self.size < other.size


class Cache:

    def __init__(self, total_size, cache_id):
        self.total_size = total_size
        self.remaining_size = total_size
        self.inserted_videos = list()
        self.cache_id = cache_id
        self.connected_ep = dict()

    def add_ep(self, ep_id, ep_lat):
        self.connected_ep[ep_id] = ep_lat

    def insert_video(self, video):
        self.inserted_videos.append(video.video_id)
        self.remaining_size = self.remaining_size - video.size

    def __lt__(self, other):
        return self.remaining_size < other.remaining_size


class EndPoint:

    def __init__(self, connected_caches, ep_lat, caches_dict):
        self.caches = connected_caches
        self.caches_dict = caches_dict
        self.requests = PriorityQueue()
        self.latency_to_datacenter = ep_lat

    def add_request(self, req):
        self.requests.put((req['times'], req['video_id']))
