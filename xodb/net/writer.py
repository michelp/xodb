from xodb.net.worker import Worker, run


class Writer(Worker):

    def handle_request(self):
        return


if __name__ == "__main__":
    run('writer', Writer)
