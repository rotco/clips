import boto3


class GlueHelper:
    def __init__(self):
        self.client = None

    def init_client(self):
        self.client = boto3.client('glue')
        return self.client

    def get_jobs(self):
        return self.client.get_jobs().get('Jobs')

    def start_job(self, job_name):
        return self.client.start_job_run(JobName=job_name)


if __name__ == '__main__':
    gl = GlueHelper()
    gl.init_client()
    jobs = gl.get_jobs()
    started_job = gl.start_job(jobs[0]['Name'])
    print(started_job)
