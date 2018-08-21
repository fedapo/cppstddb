// oci_helper.h

#ifndef OCI_HELPER_H
#define OCI_HELPER_H

#include <oci.h>
#include <string>

struct oci_error
{
  oci_error(sword st) : status(st) {}

  sword status;
};

//template <class HandleClass, int HandleType>
//using OCIHandle2 = std::unique_ptr<HandleClass, bind(OCIHandleFree(std::place_holders::_1, HandleType)>;
//using OCIHandle2 = std::unique_ptr<remove_pointer_t<HandleClass>, decltype(&OCIHandleFree)>;

template<class HandleClass, int HandleType> 
class OCIHandle
{
public:
  OCIHandle(OCIEnv* env)
  {
    sword st = OCIHandleAlloc(env, (void**)&ptr_, HandleType, 0, nullptr);

    if(st != OCI_SUCCESS)
      throw oci_error(st);
  }
  
  ~OCIHandle()
  {
    reset();
  }

  OCIHandle(const OCIHandle&) = delete;
  OCIHandle& operator=(const OCIHandle&) = delete;

  HandleClass* get() 
  {
    return ptr_;
  }

  const HandleClass* get() const
  {
    return ptr_;
  }

  void reset(HandleClass* newptr = nullptr)
  {
    if(ptr_)
      sword st = OCIHandleFree(ptr_, HandleType);
    ptr_ = newptr;
  }

  HandleClass* release()
  {
    auto tmp = ptr_;
    ptr_ = nullptr;
    return tmp;
  }

private:
  HandleClass* ptr_ = nullptr;
};

template<class DescriptorClass, int DescriptorType> 
class OCIDescriptor
{
public:
  OCIDescriptor(OCIEnv* env)
  {
    sword st = OCIDescriptorAlloc(env, (void**)&ptr_, DescriptorType, 0, nullptr);

    if(st != OCI_SUCCESS)
      throw oci_error(st);
  }

  ~OCIDescriptor()
  {
    reset();
  }

  OCIDescriptor(const OCIDescriptor&) = delete;
  OCIDescriptor& operator=(const OCIDescriptor&) = delete;

  DescriptorClass* get() const
  {
    return ptr_;
  }

  // FED ???? we need to return a reference because of OCIBindByPos and OCIDefineByPos with SQLT_RDD
  DescriptorClass*& get()
  {
    return ptr_;
  }

  void reset(DescriptorClass* newptr = nullptr)
  {
    if(ptr_)
      sword st = OCIDescriptorFree(ptr_, DescriptorType);
    ptr_ = newptr;
  }

  DescriptorClass* release()
  {
    auto tmp = ptr_;
    ptr_ = nullptr;
    return tmp;
  }

private:
  DescriptorClass* ptr_ = nullptr;
};

#endif
