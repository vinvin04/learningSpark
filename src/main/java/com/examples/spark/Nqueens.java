package com.examples.spark;

import java.util.ArrayList;
import java.util.List;

public class Nqueens {
    List<List<String>> ans;

    boolean valid(int q, int p, int[][] a) {
        for(int i = 0;i < p;i++)
            if(a[q][i] == 1) return false;
        for(int i = 0;i < q;i++)
            if(a[i][p] == 1) return false;
        if(q-1 >= 0 && p - 1 >= 0 && a[q-1][p-1] == 1) return false;
        if(q-1 >= 0 && p + 1 < a.length && a[q-1][p+1] == 1) return false;
        return true;
    }

    List<String> arrToList(int[][] a) {
        List<String> list = new ArrayList<>();
        for(int i = 0;i < a.length;i++) {
            String s = "";
            for(int j = 0;j < a.length;j++) {
                if(a[i][j] == 1)
                    s = s + "Q";
                else
                    s = s + ".";
            }
            list.add(s);
        }
        return list;
    }

    void rec(int q, int n, int[][] a) {
        if(q == n) {
            arrToList(a);
            return;
        }

        for(int i = 0;i < n;i++) {
            if(valid(q,i,a)) {
                a[q][i] = 1;
                rec(q+1,n,a);
                a[q][i] = 0;
            }
        }
    }

    public List<List<String>> solveNQueens(int n) {
        ans = new ArrayList<>();
        rec(0,n,new int[n][n]);
        return ans;
    }

    public static void main(String[] args) {
        Nqueens nqueens = new Nqueens();
        nqueens.solveNQueens(4);
    }
}
