      subroutine GET_MPI_F_STATUS_SIZE___ (CODE)
      INTEGER CODE
      CALL GET_MPI_F_STATUS_SIZE (CODE)
      END
      subroutine GET_MPI_F_STATUS_SIZE__ (CODE)
      INTEGER CODE
      CALL GET_MPI_F_STATUS_SIZE (CODE)
      END
      subroutine GET_MPI_F_STATUS_SIZE_ (CODE)
      INTEGER CODE
      CALL GET_MPI_F_STATUS_SIZE (CODE)
      END
      subroutine GET_MPI_F_STATUS_SIZE (CODE)
      INCLUDE "mpif.h"
      INTEGER CODE
      CODE = MPI_STATUS_SIZE
      RETURN
      END
