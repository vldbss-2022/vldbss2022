from http.server import HTTPServer, BaseHTTPRequestHandler

host = ('localhost', 8888)


class Resquest(BaseHTTPRequestHandler):
    def handle_cardinality_estimate(self, req_data):
        # YOUR CODE HERE: use your model in lab1
        print("cardinality_estimate post_data: " + str(req_data))
        return "0.0"

    def handle_cost_estimate(self, req_data):
        # YOUR CODE HERE: use your model in lab2
        print("cost_estimate post_data: " + str(req_data))
        return "0.0"

    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        req_data = self.rfile.read(content_length)
        resp_data = ""
        if self.path == "/cardinality":
            resp_data = self.handle_cardinality_estimate(req_data)
        elif self.path == "/cost":
            resp_data = self.handle_cost_estimate(req_data)

        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(resp_data.encode())


if __name__ == '__main__':
    server = HTTPServer(host, Resquest)
    print("Starting server, listen at: %s:%s" % host)
    server.serve_forever()
