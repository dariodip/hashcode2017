from queue import PriorityQueue


class Video:

    def __init__(self, size, video_id, end_point_id):
        self.size = size
        self.video_id = video_id
        self.end_point_id = end_point_id

    def __lt__(self, other):
        return self.size < other.size


class Cache:

    def __init__(self, total_size):
        self.total_size = total_size
        self.remaining_size = total_size
        self.inserted_videos = list()

    def insert_video(self, video):
        self.inserted_videos.append(video['id'])
        self.remaining_size = self.remaining_size - video['size']

    def __lt__(self, other):
        return self.remaining_size < other.remaining_size


class EndPoint:

    def __init__(self, connected_caches, ep_lat):
        self.caches = connected_caches
        self.requests = PriorityQueue()
        self.latency_to_datacenter = ep_lat

    def add_request(self, req):
        self.requests.put((req['times'], req['video_id']))
