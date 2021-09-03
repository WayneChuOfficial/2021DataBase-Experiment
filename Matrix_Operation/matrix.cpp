#include <iostream>
#include "p0_starter.h"
#include <vector>
using namespace std;

template <typename T>
void show(const scudb::RowMatrix<T> &matrixA)
{
    int rows = matrixA.GetRowCount();
    int cols = matrixA.GetColumnCount();
    for(int i = 0; i < rows; i++)
    {
        for(int j = 0; j < cols; j++)
        {
            cout<<matrixA.GetElement(i,j)<<" ";
        }
        cout<<endl;
    }
}

int main()
{
    scudb::RowMatrix<int> matrixA(3,3);
    scudb::RowMatrix<int> matrixB(3,3);
    cout<<"***Test RowMatrix<int> matrixA(3,3);"<<endl;
    cout<<"GetRowCount():"<<matrixA.GetRowCount()<<endl;
    cout<<"GetColumnCount:"<<matrixA.GetColumnCount()<<endl;
    try{
        cout<<"set the (20,20)th element:"<<endl;
        matrixA.SetElement(20,20,1);//out of range
        show<int>(matrixA);
    }catch(std::out_of_range &out){
        cerr << out.what()<<endl;
    }
    try{
        cout<<"set the (0,0)th element as 1"<<endl;
        matrixA.SetElement(0,0,1);//set (0,0)th element of matricxA 
        show<int>(matrixA);
    }catch(std::out_of_range &out)
    {
        cerr << out.what()<<endl;
    }
    try{
        cout<<"get the (20,0)th element:"<<endl<<matrixA.GetElement(20,0)<<endl;//out of range
    }catch(std::out_of_range &out){
        cerr << out.what()<<endl;
    }
    try{
        cout<<"get the (0,0)th element:"<<endl<<matrixA.GetElement(0,0)<<endl;
    }catch(std::out_of_range &out){
        cerr << out.what()<<endl;
    }

    cout<<"use vector to fill matrix:"<<endl;
    vector<int> vec1(8);
    for(int i = 0; i < vec1.size(); i++)
    {
        vec1[i] = i+1;
    }
    try{
        cout<<"when vector has size of 8 which is an incorrect size and vector is:"<<endl;
        for(int i = 0; i < vec1.size(); i++)
        {
            cout<<vec1[i]<<" ";
        }
        cout<<endl;
        matrixA.FillFrom(vec1);// throw out_of_range
        cout<<"matrix"<<endl;
        show<int>(matrixA);
    }catch(std::out_of_range &out)
    {
        cerr <<out.what()<<endl;
    }
    vec1.push_back(9);
    //matricxA.FillFrom(vec1);
    try{
        cout<<"when vector has size of 9 which is a correct size and vector is:";
        for(int i = 0; i < vec1.size(); i++)
        {
            cout<<vec1[i]<<" ";
        }
        cout<<endl;
        matrixA.FillFrom(vec1);
        cout<<"matrix:"<<endl;
        show<int>(matrixA);
    }catch(std::out_of_range &out)
    {
        cerr <<out.what()<<endl;
    }

    cout<<endl<<"***Test RowMatrixOperations<int>"<<endl;
    scudb::RowMatrixOperations<int> oper1;
    cout<<endl;
    cout<<"matrixA:"<<endl;
    show<int>(matrixA);
    vec1.assign(9,1);
    matrixB.FillFrom(vec1);
    cout<<"matrixB:"<<endl;
    show<int>(matrixB);
    
    unique_ptr<scudb::RowMatrix<int>> matrixC = oper1.Add(&matrixA,&matrixB);
    cout<<"matrixC = Add(matrixA,matrixB):"<<endl;
    show<int>(*matrixC);

    unique_ptr<scudb::RowMatrix<int>> matrixD = oper1.Multiply(&matrixA,&matrixB);
    cout<<"matrixD = Multiply(matrixA,matrixB):"<<endl;
    show<int>(*matrixD);

    unique_ptr<scudb::RowMatrix<int>> matrixE = oper1.GEMM(&matrixA,&matrixB,&*matrixC);
    cout<<"matrixD = GEMM(matrixA,matrixB,matrixC):"<<endl;
    show<int>(*matrixE);
    return 0;
}