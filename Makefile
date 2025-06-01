generate:
	mkdir -p generated


	poetry run python3 -m grpc_tools.protoc \
		-I./ \
		--python_out=./generated \
		--grpc_python_out=./generated \
		--pyi_out=./generated \
		./events.proto

clean:
	rm -rf generated/*