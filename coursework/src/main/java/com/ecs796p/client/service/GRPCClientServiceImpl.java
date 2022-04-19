package com.ecs796p.client.service;

import com.ecs796p.exception.MatrixException;
import com.ecs796p.proto.BlockMultServiceGrpc;
import com.ecs796p.utils.MatrixUtils;
import com.ecs796p.proto.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;


@Service
public class GRPCClientServiceImpl implements GRPCClientService{

	long deadline;

	private int[] stubPorts = {8081,8082,8083,8084,8085,8086,8087,8088};

	@Value("${serverAddress}")
	private String serverAddress;

	private ManagedChannel[] channels;
	private BlockMultServiceGrpc.BlockMultServiceBlockingStub[] stubs;
	private BlockingQueue<Integer> stubIndices = new LinkedBlockingQueue<>(stubPorts.length);

	@PostConstruct
	public void init() throws InterruptedException {
		channels = createChannels();
		stubs = createStubs();
	}

	@PreDestroy
	public void destroy() {
	    for(ManagedChannel channel : channels) {
			channel.shutdown();
		}
	}

	@Override
	public String multiplyMatrixFiles(String matrixStringA, String matrixStringB, long deadline) throws MatrixException, ExecutionException, InterruptedException {
		int[][] A = MatrixUtils.stringToMatrixArray(matrixStringA);
		int[][] B = MatrixUtils.stringToMatrixArray(matrixStringB);
		System.out.println("Matrix 1: " + MatrixUtils.encodeMatrix(A));
		System.out.println("Matrix 2: " + MatrixUtils.encodeMatrix(B));
		int[][] multipliedMatrixBlock = multiplyMatrixBlock(A, B, deadline);
		return MatrixUtils.encodeMatrix(multipliedMatrixBlock);
	}

	@Override
	public String addMatrixFiles(String matrixStringA, String matrixStringB, long deadline) throws MatrixException, ExecutionException, InterruptedException {
		int[][] A = MatrixUtils.stringToMatrixArray(matrixStringA);
		int[][] B = MatrixUtils.stringToMatrixArray(matrixStringB);
		System.out.println("Matrix 1: " + MatrixUtils.encodeMatrix(A));
		System.out.println("Matrix 2: " + MatrixUtils.encodeMatrix(B));
		int[][] multipliedMatrixBlock = addMatrixBlock(A, B, deadline);
		return MatrixUtils.encodeMatrix(multipliedMatrixBlock);
	}


	/**
	 * Multiplies matrices using addBlock and multiplyBlock
	 */
	private int[][] multiplyMatrixBlock(int[][] A, int[][] B, long deadline) throws InterruptedException, ExecutionException {

		// split matrix blocks into 8 smaller blocks
		HashMap<String, int[][]> blocks = MatrixUtils.splitBlocks(A, B);

		// get first gRPC server stub
		int firstStubIndex = takeStubIndices(1)[0];

		// footprint algorithm to see how long first call takes
		long startTime = System.nanoTime();

		// CompletableFuture enables asynchronous calls to the multiplyBlock function
		CompletableFuture<int[][]> A1A2Future = CompletableFuture.supplyAsync(() -> multiplyBlock(blocks.get("A1"), blocks.get("A2"), firstStubIndex));

		// This will wait for the async function to complete before continuing
		int[][] A1A2 = A1A2Future.get();
		long endTime = System.nanoTime();
		long footprint= endTime-startTime;

		// remaining block calls
		long numBlockCalls = 11L;
		int numberServer = (int) Math.ceil((float)footprint*(float)numBlockCalls/(float)deadline);
		numberServer = numberServer <= 8 ? numberServer : 8;

		System.out.println("Using "+ numberServer + " servers for rest of calculation");

		// take the least recently used stub indices for this workload to reduce traffic
		int[] indices = takeStubIndices(numberServer);

		// a thread safe index queue so each the async functions are evenly spread along the stubs
		BlockingQueue<Integer> indexQueue = new LinkedBlockingQueue<>((int) numBlockCalls);

		int i = 0;
		while(indexQueue.size() != numBlockCalls) {
		    if(indices.length == i) {
		        i = 0;
			}
			indexQueue.add(indices[i]);
			i++;
		}

		// a series of asynchronous calls to the gRPC blocking calls
		// does run asynchronously as you can sometimes see the addblock function calls before some multiplication call
		CompletableFuture<int[][]> B1C2 = CompletableFuture.supplyAsync(() -> {
			try {
				return multiplyBlock(blocks.get("B1"), blocks.get("C2"), indexQueue.take());
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			return null;
		});

		CompletableFuture<int[][]> A1B2 = CompletableFuture.supplyAsync(() -> {
			try {
				return multiplyBlock(blocks.get("A1"), blocks.get("B2"), indexQueue.take());
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			return null;
		});

		CompletableFuture<int[][]> B1D2 = CompletableFuture.supplyAsync(() -> {
			try {
				return multiplyBlock(blocks.get("B1"), blocks.get("D2"), indexQueue.take());
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			return null;
		});

		CompletableFuture<int[][]> C1A2 = CompletableFuture.supplyAsync(() -> {
			try {
				return multiplyBlock(blocks.get("C1"), blocks.get("A2"), indexQueue.take());
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			return null;
		});

		CompletableFuture<int[][]> D1C2 = CompletableFuture.supplyAsync(() -> {
			try {
				return multiplyBlock(blocks.get("D1"), blocks.get("C2"), indexQueue.take());
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			return null;
		});

		CompletableFuture<int[][]> C1B2 = CompletableFuture.supplyAsync(() -> {
			try {
				return multiplyBlock(blocks.get("C1"), blocks.get("B1"), indexQueue.take());
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			return null;
		});

		CompletableFuture<int[][]> D1D2 = CompletableFuture.supplyAsync(() -> {
			try {
				return multiplyBlock(blocks.get("D1"), blocks.get("D2"), indexQueue.take());
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			return null;
		});

		CompletableFuture<int[][]> A3 = CompletableFuture.supplyAsync(() -> {
			try {
				return addBlock(A1A2, B1C2.get(), indexQueue.take());
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
			return null;
		});

		CompletableFuture<int[][]> B3 = CompletableFuture.supplyAsync(() -> {
			try {
				return addBlock(A1B2.get(), B1D2.get(), indexQueue.take());
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
			return null;
		});

		CompletableFuture<int[][]> C3 = CompletableFuture.supplyAsync(() -> {
			try {
				return addBlock(C1A2.get(), D1C2.get(), indexQueue.take());
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
			return null;
		});

		CompletableFuture<int[][]> D3 = CompletableFuture.supplyAsync(() -> {
			try {
				return addBlock(C1B2.get(), D1D2.get(), indexQueue.take());
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
			return null;
		});

		// join the remote calculations back together

		int[][] res = MatrixUtils.joinBlocks(A3.get(), B3.get(), C3.get(), D3.get());

		System.out.println("Finished calculation");
		return res;
	}

	/**
	 * Add matrices using addBlock
	 */
	private int[][] addMatrixBlock(int[][] A, int[][] B, long deadline) throws InterruptedException, ExecutionException {

		// split matrix blocks into 8 smaller blocks
		HashMap<String, int[][]> blocks = MatrixUtils.splitBlocks(A, B);

		// get first gRPC server stub
		int firstStubIndex = takeStubIndices(1)[0];

		// footprint algorithm to see how long first call takes
		long startTime = System.nanoTime();

		// CompletableFuture enables asynchronous calls to the multiplyBlock function
		CompletableFuture<int[][]> A3 = CompletableFuture.supplyAsync(() ->
				addBlock(blocks.get("A1"), blocks.get("A2"), firstStubIndex));

		// This will wait for the async function to complete before continuing
		int [][]  Atest= A3.get();
		long endTime = System.nanoTime();
		long footprint= endTime-startTime;

		// remaining block calls
		long numBlockCalls = 3L;
		int numberServer = (int) Math.ceil((float)footprint*(float)numBlockCalls/(float)deadline);
		numberServer = numberServer <= 8 ? numberServer : 8;

		System.out.println("Using "+ numberServer + " servers for rest of calculation");

		// take the least recently used stub indices for this workload to reduce traffic
		int[] indices = takeStubIndices(numberServer);

		// a thread safe index queue so each the async functions are evenly spread along the stubs
		BlockingQueue<Integer> indexQueue = new LinkedBlockingQueue<>((int) numBlockCalls);

		int i = 0;
		while(indexQueue.size() != numBlockCalls) {
			if(indices.length == i) {
				i = 0;
			}
			indexQueue.add(indices[i]);
			i++;
		}

		// a series of asynchronous calls to the gRPC blocking calls
		// does run asynchronously as you can sometimes see the addblock function calls before some multiplication call

		CompletableFuture<int[][]> B3 = CompletableFuture.supplyAsync(() -> {
			try {
				return addBlock(blocks.get("B1"), blocks.get("B2"), indexQueue.take());
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			return null;
		});

		CompletableFuture<int[][]> C3 = CompletableFuture.supplyAsync(() -> {
			try {
				return addBlock(blocks.get("C1"), blocks.get("C2"), indexQueue.take());
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			return null;
		});

		CompletableFuture<int[][]> D3 = CompletableFuture.supplyAsync(() -> {
			try {
				return addBlock(blocks.get("D1"), blocks.get("D2"), indexQueue.take());
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			return null;
		});

		int[][] res = MatrixUtils.joinBlocks(A3.get(), B3.get(), C3.get(), D3.get());

		System.out.println("Finished calculation");
		return res;
	}


	/**
	 * Takes the indices of the stubs that have not been used recently and adds them to the back of the queue.
	 * @param num
	 * @return indices of the num stubs
	 * @throws InterruptedException
	 */
	private int[] takeStubIndices(int num) throws InterruptedException {
		int[] indices = new int[num];
		for(int i = 0; i < num; i++) {
			indices[i] = this.stubIndices.take();
			this.stubIndices.add(indices[i]);
		}
		return indices;
	}

	private BlockMultServiceGrpc.BlockMultServiceBlockingStub[] createStubs() {
		BlockMultServiceGrpc.BlockMultServiceBlockingStub[] stubs = new BlockMultServiceGrpc.BlockMultServiceBlockingStub[stubPorts.length];

		for(int i =0; i < channels.length; i++) {
			stubs[i] = BlockMultServiceGrpc.newBlockingStub(channels[i]);
		}

		for(int i = 0; i < stubPorts.length; i++) {
			stubIndices.add(i);
		}

		return stubs;
	}



	private ManagedChannel[] createChannels() {
		ManagedChannel[] chans = new ManagedChannel[stubPorts.length];
		System.out.println("Connecting to server at: " + serverAddress);

		for(int i =0; i < stubPorts.length; i++) {
			chans[i] = ManagedChannelBuilder.forAddress(serverAddress, stubPorts[i])
					.keepAliveWithoutCalls(true)
					.usePlaintext()
					.build();
		}
		return chans;
	}

	/**
	 * Add integer matrices via gRPC
	 */

	private int[][] addBlock(int A[][], int B[][], int stubIndex) {
		System.out.println("Calling addBlock on server " + (stubIndex + 1));
		MatrixRequest request = generateRequest(A, B);
		MatrixResponse matrixAddResponse = this.stubs[stubIndex].addBlock(request);
		int[][] summedMatrix = MatrixUtils.decodeMatrix(matrixAddResponse.getMatrix());
		return summedMatrix;
	}


	/**
	 * Multiply integer matrices via gRPC
	 */
	private int[][] multiplyBlock(int A[][], int B[][], int stubIndex) {
		System.out.println("Calling multiplyBlock on server " + (stubIndex+1));
		MatrixRequest request = generateRequest(A, B);
		MatrixResponse matrixMultiplyResponse = this.stubs[stubIndex].multiplyBlock(request);
		int[][] multipliedMatrix = MatrixUtils.decodeMatrix(matrixMultiplyResponse.getMatrix());
		return multipliedMatrix;
	}


	/**
	 *  encode the matrices and return a MatrixRequest object
	 */
	private static MatrixRequest generateRequest(int A[][], int B[][]) {
		String matrixA = MatrixUtils.encodeMatrix(A);
		String matrixB = MatrixUtils.encodeMatrix(B);

		MatrixRequest request = MatrixRequest.newBuilder()
				.setMatrixA(matrixA)
				.setMatrixB(matrixB)
				.build();

		return request;
	}


}
