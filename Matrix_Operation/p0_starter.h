//===----------------------------------------------------------------------===//
//
//
//
// p0_starter.h
//
// Identification: src/include/primer/p0_starter.h
//
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <stdexcept>
#include <vector>

namespace scudb {

/**
 * The Matrix type defines a common
 * interface for matrix operations.
 */
template <typename T>
class Matrix {
 protected:
  /**
   * TODO(P0): Add implementation
   * 
   * Construct a new Matrix instance.
   * @param rows The number of rows
   * @param cols The number of columns
   *
   */
  Matrix(int rows, int cols) {
    this->rows_ = rows;
    this->cols_ = cols;
    this->linear_ = new T[rows_*cols_];
    for(int i = 0; i < rows_*cols_; i++)
    {
      linear_[i] = 0;
    }
  }

  /** The number of rows in the matrix */
  int rows_;
  /** The number of columns in the matrix */
  int cols_;

  /**
   * TODO(P0): Allocate the array in the constructor.
   * TODO(P0): Deallocate the array in the destructor.
   * A flattened array containing the elements of the matrix.
   */
  T *linear_;

 public:
  /** @return The number of rows in the matrix */
  virtual int GetRowCount() const = 0;

  /** @return The number of columns in the matrix */
  virtual int GetColumnCount() const = 0;

  /**
   * Get the (i,j)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @return The (i,j)th matrix element
   * @throws OUT_OF_RANGE if either index is out of range
   */
  virtual T GetElement(int i, int j) const = 0;

  /**
   * Set the (i,j)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @param val The value to insert
   * @throws OUT_OF_RANGE if either index is out of range
   */
  virtual void SetElement(int i, int j, T val) = 0;

  /**
   * Fill the elements of the matrix from `source`.
   *
   * Throw OUT_OF_RANGE in the event that `source`
   * does not contain the required number of elements.
   *
   * @param source The source container
   * @throws OUT_OF_RANGE if `source` is incorrect size
   */
  virtual void FillFrom(const std::vector<T> &source) = 0;

  /**
   * Destroy a matrix instance.
   * TODO(P0): Add implementation
   */
  //virtual ~Matrix() = default;
  virtual ~Matrix(){
    delete this->linear_;
  }
};

/**
 * The RowMatrix type is a concrete matrix implementation.
 * It implements the interface defined by the Matrix type.
 */
template <typename T>
class RowMatrix : public Matrix<T> {
 public:
  /**
   * TODO(P0): Add implementation
   *
   * Construct a new RowMatrix instance.
   * @param rows The number of rows
   * @param cols The number of columns
   */
  RowMatrix(int rows, int cols) : Matrix<T>(rows, cols) {
    data_ = new T*[rows];
    for(int i = 0; i < rows; i++)
    {
      data_[i] = &this->linear_[i*cols];
    }
  }

  /**
   * TODO(P0): Add implementation
   * @return The number of rows in the matrix
   */
  int GetRowCount() const override { return this->rows_; }

  /**
   * TODO(P0): Add implementation
   * @return The number of columns in the matrix
   */
  int GetColumnCount() const override { return this->cols_; }

  /**
   * TODO(P0): Add implementation
   *
   * Get the (i,j)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @return The (i,j)th matrix element
   * @throws OUT_OF_RANGE if either index is out of range
   */
T GetElement(int i, int j) const override {
    //throw NotImplementedException{"RowMatrix::GetElement() not implemented."};
    if(i<0||j<0||i>=this->rows_||j>=this->cols_)
    {
      throw std::out_of_range{"Error::GetElement(): The index of the element is out of range in the matrix!"};
    }else{
      return data_[i][j];
    }
  }

  /**
   * Set the (i,j)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @param val The value to insert
   * @throws OUT_OF_RANGE if either index is out of range
   */
  void SetElement(int i, int j, T val) override {
    if(i<0||j<0||i>=this->rows_||j>=this->cols_)
    {
      throw std::out_of_range{"Error::SetElement(): The index of the element you choose is out of range!"};
    }else{
      data_[i][j] = val;
    }
  }

  /**
   * TODO(P0): Add implementation
   *
   * Fill the elements of the matrix from `source`.
   *
   * Throw OUT_OF_RANGE in the event that `source`
   * does not contain the required number of elements.
   *
   * @param source The source container
   * @throws OUT_OF_RANGE if `source` is incorrect size
   */
  void FillFrom(const std::vector<T> &source) override {
    //throw NotImplementedException{"RowMatrix::FillFrom() not implemented."};
  if(source.size()!=this->rows_*this->cols_)
    {
      throw std::out_of_range{"Error:: FillFrom(): The vector has an incorrect size!"};
    }else{
      for(int i = 0; i < source.size(); i++)
      {
        this->linear_[i] = source[i];
      }
    }
  }

  /**
   * TODO(P0): Add implementation
   *
   * Destroy a RowMatrix instance.
   */
  ~RowMatrix() override {
    delete this->data_;
  };

 private:
  /**
   * A 2D array containing the elements of the matrix in row-major format.
   *
   * TODO(P0):
   * - Allocate the array of row pointers in the constructor.
   * - Use these pointers to point to corresponding elements of the `linear` array.
   * - Don't forget to deallocate the array in the destructor.
   */
  T **data_;
};

/**
 * The RowMatrixOperations class defines operations
 * that may be performed on instances of `RowMatrix`.
 */
template <typename T>
class RowMatrixOperations {
 public:
  /**
   * Compute (`matrixA` + `matrixB`) and return the result.
   * Return `nullptr` if dimensions mismatch for input matrices.
   * @param matrixA Input matrix
   * @param matrixB Input matrix
   * @return The result of matrix addition
   */
  static std::unique_ptr<RowMatrix<T>> Add(const RowMatrix<T> *matrixA, const RowMatrix<T> *matrixB) {
    // TODO(P0): Add implementation
    if(matrixA->GetRowCount() == matrixB->GetRowCount() && matrixA->GetColumnCount() == matrixB->GetColumnCount())
    {
      int rows = matrixA->GetRowCount();
      int cols = matrixA->GetColumnCount();
      std::unique_ptr<RowMatrix<T>> p (new RowMatrix<T>(rows,cols));
      for(int i = 0; i < rows; i++)
      {
        for(int j = 0; j < cols; j++)
        {
          T a = matrixA->GetElement(i,j);
          T b = matrixB->GetElement(i,j);
          p->SetElement(i,j,a+b);
        }
      }
      return p;
    }else{
      return std::unique_ptr<RowMatrix<T>>(nullptr);
    }
  }

  /**
   * Compute the matrix multiplication (`matrixA` * `matrixB` and return the result.
   * Return `nullptr` if dimensions mismatch for input matrices.
   * @param matrixA Input matrix
   * @param matrixB Input matrix
   * @return The result of matrix multiplication
   */
  static std::unique_ptr<RowMatrix<T>> Multiply(const RowMatrix<T> *matrixA, const RowMatrix<T> *matrixB) {
    // TODO(P0): Add implementation
    if(matrixA->GetColumnCount() == matrixB->GetRowCount())
    {
      int rows = matrixA->GetRowCount();
      int cols = matrixB->GetColumnCount();
      int mid = matrixA->GetColumnCount();
      std::unique_ptr<RowMatrix<T>> p (new RowMatrix<T>(rows,cols));
      for(int j = 0; j < cols; j++)
      {
        for(int i = 0;i < rows; i++)
        {
          T tem = 0;
          for(int k = 0; k < mid; k++)
          {
            tem += matrixA->GetElement(i,k)*matrixB->GetElement(k,j);
          }
          p->SetElement(i,j,tem);
        }
      }
      return p;
    }else{
      return std::unique_ptr<RowMatrix<T>>(nullptr);
    }
  }

  /**
   * Simplified General Matrix Multiply operation. Compute (`matrixA` * `matrixB` + `matrixC`).
   * Return `nullptr` if dimensions mismatch for input matrices.
   * @param matrixA Input matrix
   * @param matrixB Input matrix
   * @param matrixC Input matrix
   * @return The result of general matrix multiply
   */
  static std::unique_ptr<RowMatrix<T>> GEMM(const RowMatrix<T> *matrixA, const RowMatrix<T> *matrixB,
                                            const RowMatrix<T> *matrixC) {
    // TODO(P0): Add implementation
    if(matrixA->GetColumnCount() == matrixB->GetRowCount() && matrixA->GetRowCount() == matrixC->GetRowCount() 
    && matrixB->GetColumnCount() == matrixC->GetColumnCount())
    {
      int rows = matrixA->GetRowCount();
      int cols = matrixB->GetColumnCount();
      int mid = matrixA->GetColumnCount();
      std::unique_ptr<RowMatrix<T>> p (new RowMatrix<T>(rows,cols));
      for(int j = 0; j < cols; j++)
      {
        for(int i = 0;i < rows; i++)
        {
          T tem = 0;
          for(int k = 0; k < mid; k++)
          {
            tem += matrixA->GetElement(i,k)*matrixB->GetElement(k,j);
          }
          T c = matrixC->GetElement(i,j);
          p->SetElement(i,j,tem + c);
        }
      }
      return p;
    }else{
      return std::unique_ptr<RowMatrix<T>>(nullptr);
    }
  }
};
}  // namespace scudb
