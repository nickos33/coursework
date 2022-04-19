package com.ecs796p.server;

import com.ecs796p.proto.BlockMultServiceGrpc;
import com.ecs796p.proto.*;
import io.grpc.stub.StreamObserver;
import com.ecs796p.utils.MatrixUtils;

import javax.el.MethodNotFoundException;

public class BlockMultServiceImpl extends BlockMultServiceGrpc.BlockMultServiceImplBase {

	int threadNumber;

	public BlockMultServiceImpl(int threadNumber) {
		this.threadNumber = threadNumber;
	}

	@Override
	public void addBlock(MatrixRequest request, StreamObserver<MatrixResponse> responseObserver) {
		System.out.println("addBlock called on server "+ threadNumber);
		requestHandler(request, responseObserver, "add");
	}

	@Override
	public void multiplyBlock(MatrixRequest request, StreamObserver<MatrixResponse> responseObserver) {
		System.out.println("multiplyBlock called on server " + threadNumber);
		requestHandler(request, responseObserver, "multiply");
	}

	/**
	 * Handles the gRPC request for both addBlock and multiplyBlock methods
	 */
	private void requestHandler(MatrixRequest request, StreamObserver<MatrixResponse> responseObserver, String operation) throws MethodNotFoundException {

		// decode matrixA and matrixB from the request
		int[][] decodedMatrixA = MatrixUtils.decodeMatrix(request.getMatrixA());
		int[][] decodedMatrixB = MatrixUtils.decodeMatrix(request.getMatrixB());

		// define the matrix operation result to be of size MAX, MAX
		int[][] result;

		switch(operation) {
			case "add":
				result = MatrixUtils.addMatrices(decodedMatrixA, decodedMatrixB);
				break;
			case "multiply":
				result = MatrixUtils.multiplyMatrices(decodedMatrixA, decodedMatrixB);
				break;
			default:
				System.out.println("Cannot recognise operation: " + operation);
				throw new MethodNotFoundException("Couldn't find method: " + operation);
		}

		// encode the resultant matrix as a string
		String encodedMatrix = MatrixUtils.encodeMatrix(result);

		// generate the matrix response object
		MatrixResponse response = MatrixResponse.newBuilder()
			.setMatrix(encodedMatrix)
			.build();

		// send response of gRPC
		responseObserver.onNext(response);
		responseObserver.onCompleted();
	}


}
