package com.ecs796p.client.service;

import com.ecs796p.exception.MatrixException;

import java.util.concurrent.ExecutionException;

public interface GRPCClientService {

    String multiplyMatrixFiles(String matrix1String, String matrix2String, long parseLong) throws MatrixException, ExecutionException, InterruptedException;

    String addMatrixFiles(String matrix1String, String matrix2String, long parseLong) throws MatrixException, ExecutionException, InterruptedException;
}
