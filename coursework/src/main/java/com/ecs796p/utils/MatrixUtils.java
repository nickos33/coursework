package com.ecs796p.utils;


import com.ecs796p.exception.MatrixException;

import java.util.Arrays;
import java.util.HashMap;

import static com.google.common.math.IntMath.isPowerOfTwo;

public class MatrixUtils {
    /**
     * Takes a matrix string turns it into a 2D integer array
     */
    public static int[][] decodeMatrix(String matrixString) {
        return stringToDeep(matrixString);
    }

    /**
     * Takes a 2D integer array and turns it into an encoded string
     */
    public static String encodeMatrix(int[][] matrix) {
        return Arrays.deepToString(matrix);
    }

    /**
     * Turns array.toDeepString() back to array[][]
     */
    public static int[][] stringToDeep(String str) {
        int row = 0;
        int col = 0;
        for (int i = 0; i < str.length(); i++) {
            if (str.charAt(i) == '[') {
                row++;
            }
        }
        row--;
        for (int i = 0;; i++) {
            if (str.charAt(i) == ',') {
                col++;
            }
            if (str.charAt(i) == ']') {
                break;
            }
        }
        col++;

        int[][] out = new int[row][col];

        str = str.replaceAll("\\[", "").replaceAll("\\]", "");

        String[] s1 = str.split(", ");

        int j = -1;
        for (int i = 0; i < s1.length; i++) {
            if (i % col == 0) {
                j++;
            }
            out[j][i % col] = Integer.parseInt(s1[i]);
        }
        return out;
    }

    /**
     * Sums two matrices
     */
    public static int[][] addMatrices(int[][] matrixA, int[][] matrixB) {

        int MAX = matrixA.length;
        int[][] result = new int[MAX][MAX];

        for (int i=0; i<result.length; i++) {
            for (int j=0; j < result.length; j++) {
                result[i][j] = matrixA[i][j] + matrixB[i][j];
            }
        }
        return result;
    }

    /**
     * Multiply two matrices together
     */
    public static int[][] multiplyMatrices(int A[][], int B[][]) {

        int MAX = A.length;
        int blockSize = MAX/2;
        int C[][]= new int[MAX][MAX];

        for(int i=0;i<blockSize;i++){
            for(int j=0;j<blockSize;j++){
                for(int k=0;k<blockSize;k++){
                    C[i][j]+=(A[i][k]*B[k][j]);
                }
            }
        }

        return C;
    }

    public static HashMap<String,int[][]> splitBlocks(int[][] A, int[][] B) {

        int MAX = A.length;
        int bSize = MAX/2;

        int[][] A1 = new int[MAX][MAX];
        int[][] A2 = new int[MAX][MAX];
        int[][] B1 = new int[MAX][MAX];
        int[][] B2 = new int[MAX][MAX];
        int[][] C1 = new int[MAX][MAX];
        int[][] C2 = new int[MAX][MAX];
        int[][] D1 = new int[MAX][MAX];
        int[][] D2 = new int[MAX][MAX];

        for (int i = 0; i < bSize; i++)
        {
            for (int j = 0; j < bSize; j++)
            {
                A1[i][j]=A[i][j];
                A2[i][j]=B[i][j];
            }
        }
        for (int i = 0; i < bSize; i++)
        {
            for (int j = bSize; j < MAX; j++)
            {
                B1[i][j-bSize]=A[i][j];
                B2[i][j-bSize]=B[i][j];
            }
        }
        for (int i = bSize; i < MAX; i++)
        {
            for (int j = 0; j < bSize; j++)
            {
                C1[i-bSize][j]=A[i][j];
                C2[i-bSize][j]=B[i][j];
            }
        }
        for (int i = bSize; i < MAX; i++)
        {
            for (int j = bSize; j < MAX; j++)
            {
                D1[i-bSize][j-bSize]=A[i][j];
                D2[i-bSize][j-bSize]=B[i][j];
            }
        }
        HashMap<String, int[][]> blocks = new HashMap<>();
        blocks.put("A1", A1);
        blocks.put("A2", A2);
        blocks.put("B1", B1);
        blocks.put("B2", B2);
        blocks.put("C1", C1);
        blocks.put("C2", C2);
        blocks.put("D1", D1);
        blocks.put("D2", D2);
        return blocks;
    }

    public static int[][] joinBlocks(int[][] A3, int[][] B3, int[][] C3, int[][] D3) {
        int MAX = A3.length;
        int bSize = MAX/2;
        int[][] res = new int[MAX][MAX];

        for (int i = 0; i < bSize; i++)
        {
            for (int j = 0; j < bSize; j++)
            {
                res[i][j]=A3[i][j];
            }
        }
        for (int i = 0; i < bSize; i++)
        {
            for (int j = bSize; j < MAX; j++)
            {
                res[i][j]=B3[i][j-bSize];
            }
        }
        for (int i = bSize; i < MAX; i++)
        {
            for (int j = 0; j < bSize; j++)
            {
                res[i][j]=C3[i-bSize][j];
            }
        }
        for (int i = bSize; i < MAX; i++)
        {
            for (int j = bSize; j < MAX; j++)
            {
                res[i][j]=D3[i-bSize][j-bSize];
            }
        }
        return res;
    }

    public static int[][] stringToMatrixArray(String matrixString) throws MatrixException {
        // convert matrix string to lines and columns
        String[] lines = matrixString.trim().split("\n");
        String[] columns = lines[0].trim().split(" ");

        // init the matrix array
        int[][] matrixArray = new int[lines.length][columns.length];

        if(lines.length < 1 || columns.length < 1) {
            throw new MatrixException("Invalid matrix:\n\n" + matrixString + "\n\nmatrix must have rows and columns", new Error("matrix must have rows and columns"));
        }

        if(lines.length != columns.length) {
            throw new MatrixException("Invalid matrix:\n\n" + matrixString + "\n\nmatrix must same number of rows and columns", new Error("matrix must same number of rows and columns"));
        }

        if(!isPowerOfTwo(lines.length) || !isPowerOfTwo(columns.length)){
            throw new MatrixException("Invalid matrix:\n\n" + matrixString + "\n\nmatrix row and column size must be a power of 2", new Error("matrix row and column size must be a power of 2"));
        }

        try {
            // loop through each matrix value and assign to matrixArray
            for (int i = 0; i < lines.length; i++) {
                String[] matrixValues = lines[i].trim().split(" ");
                if(matrixValues.length != columns.length) {
                    throw new MatrixException("Invalid matrix:\n\n" + matrixString + "\n\nmatrix row length not equal", new Error("matrix row length not equal"));
                }
                for (int j = 0; j < matrixValues.length; j++) {
                    matrixArray[i][j] = Integer.parseInt(matrixValues[j]);
                }
            }
        } catch (NumberFormatException|ArrayIndexOutOfBoundsException e) {
            throw new MatrixException("Invalid matrix:\n\n" + matrixString, e);
        }

        return matrixArray;
    }

}
