"""
Dynamic Database Manager - Supports Multiple Database Backends
Supports: File-based storage, MongoDB, and extensible to other databases
"""

import json
import os
from typing import Optional, Dict, Any, List
from datetime import datetime
from abc import ABC, abstractmethod
import shutil


class DatabaseBackend(ABC):
    """Abstract base class for database backends"""
    
    @abstractmethod
    def create_user(self, user_id: str, user_data: Dict[str, Any]) -> None:
        """Create a new user"""
        pass
    
    @abstractmethod
    def get_user(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Get user by ID"""
        pass
    
    @abstractmethod
    def update_user(self, user_id: str, user_data: Dict[str, Any]) -> None:
        """Update user data"""
        pass
    
    @abstractmethod
    def delete_user(self, user_id: str) -> None:
        """Delete user"""
        pass
    
    @abstractmethod
    def get_user_by_email(self, email: str) -> Optional[Dict[str, Any]]:
        """Get user by email"""
        pass
    
    @abstractmethod
    def create_class(self, teacher_id: str, class_id: str, class_data: Dict[str, Any]) -> None:
        """Create a new class"""
        pass
    
    @abstractmethod
    def get_class(self, class_id: str) -> Optional[Dict[str, Any]]:
        """Get class by ID"""
        pass
    
    @abstractmethod
    def update_class(self, class_id: str, class_data: Dict[str, Any]) -> None:
        """Update class data"""
        pass
    
    @abstractmethod
    def delete_class(self, class_id: str) -> None:
        """Delete class"""
        pass
    
    @abstractmethod
    def get_all_classes(self, teacher_id: str) -> List[Dict[str, Any]]:
        """Get all classes for a teacher"""
        pass
    
    @abstractmethod
    def create_student(self, student_id: str, student_data: Dict[str, Any]) -> None:
        """Create a new student"""
        pass
    
    @abstractmethod
    def get_student(self, student_id: str) -> Optional[Dict[str, Any]]:
        """Get student by ID"""
        pass
    
    @abstractmethod
    def update_student(self, student_id: str, student_data: Dict[str, Any]) -> None:
        """Update student data"""
        pass
    
    @abstractmethod
    def create_enrollment(self, enrollment_data: Dict[str, Any]) -> None:
        """Create enrollment record"""
        pass
    
    @abstractmethod
    def get_enrollments(self, class_id: str) -> List[Dict[str, Any]]:
        """Get all enrollments for a class"""
        pass
    
    @abstractmethod
    def update_enrollment(self, class_id: str, student_id: str, enrollment_data: Dict[str, Any]) -> None:
        """Update enrollment status"""
        pass
    
    @abstractmethod
    def save_contact_message(self, message_data: Dict[str, Any]) -> None:
        """Save contact form message"""
        pass
    
    @abstractmethod
    def get_contact_messages(self, email: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get contact messages"""
        pass
    
    @abstractmethod
    def create_qr_session(self, class_id: str, date: str, session_data: Dict[str, Any]) -> None:
        """Create QR session"""
        pass
    
    @abstractmethod
    def get_qr_session(self, class_id: str, date: str) -> Optional[Dict[str, Any]]:
        """Get QR session"""
        pass
    
    @abstractmethod
    def update_qr_session(self, class_id: str, date: str, session_data: Dict[str, Any]) -> None:
        """Update QR session"""
        pass


class MongoDBBackend(DatabaseBackend):
    """MongoDB implementation of database backend"""
    
    def __init__(self, connection_string: str, database_name: str = "lernova_db"):
        """Initialize MongoDB connection"""
        try:
            from pymongo import MongoClient
            from pymongo.errors import ConnectionFailure
        except ImportError:
            raise ImportError("pymongo is required for MongoDB backend. Install with: pip install pymongo")
        
        self.client = MongoClient(connection_string)
        self.db = self.client[database_name]
        
        # Test connection
        try:
            self.client.admin.command('ping')
            print(f"✅ MongoDB connected successfully to database: {database_name}")
        except ConnectionFailure:
            raise ConnectionFailure("Failed to connect to MongoDB")
        
        # Collections
        self.users = self.db['users']
        self.classes = self.db['classes']
        self.students = self.db['students']
        self.enrollments = self.db['enrollments']
        self.contact_messages = self.db['contact_messages']
        self.qr_sessions = self.db['qr_sessions']
        
        # Create indexes
        self._create_indexes()
    
    def _create_indexes(self):
        """Create database indexes for better performance"""
        # Users
        self.users.create_index("email", unique=True)
        self.users.create_index("user_id", unique=True)
        
        # Classes
        self.classes.create_index("class_id", unique=True)
        self.classes.create_index("teacher_id")
        
        # Students
        self.students.create_index("student_id", unique=True)
        self.students.create_index("email")
        
        # Enrollments
        self.enrollments.create_index([("class_id", 1), ("student_id", 1)])
        
        # QR Sessions
        self.qr_sessions.create_index([("class_id", 1), ("date", 1)])
        
        print("✅ MongoDB indexes created")
    
    def create_user(self, user_id: str, user_data: Dict[str, Any]) -> None:
        """Create a new user"""
        user_data['user_id'] = user_id
        user_data['created_at'] = datetime.utcnow().isoformat()
        self.users.insert_one(user_data)
    
    def get_user(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Get user by ID"""
        user = self.users.find_one({"user_id": user_id}, {"_id": 0})
        return user
    
    def update_user(self, user_id: str, user_data: Dict[str, Any]) -> None:
        """Update user data"""
        user_data['updated_at'] = datetime.utcnow().isoformat()
        self.users.update_one(
            {"user_id": user_id},
            {"$set": user_data}
        )
    
    def delete_user(self, user_id: str) -> None:
        """Delete user"""
        self.users.delete_one({"user_id": user_id})
    
    def get_user_by_email(self, email: str) -> Optional[Dict[str, Any]]:
        """Get user by email"""
        user = self.users.find_one({"email": email}, {"_id": 0})
        return user
    
    def create_class(self, teacher_id: str, class_id: str, class_data: Dict[str, Any]) -> None:
        """Create a new class"""
        class_data['class_id'] = class_id
        class_data['teacher_id'] = teacher_id
        class_data['created_at'] = datetime.utcnow().isoformat()
        self.classes.insert_one(class_data)
    
    def get_class(self, class_id: str) -> Optional[Dict[str, Any]]:
        """Get class by ID"""
        cls = self.classes.find_one({"class_id": class_id}, {"_id": 0})
        return cls
    
    def update_class(self, class_id: str, class_data: Dict[str, Any]) -> None:
        """Update class data"""
        class_data['updated_at'] = datetime.utcnow().isoformat()
        self.classes.update_one(
            {"class_id": class_id},
            {"$set": class_data}
        )
    
    def delete_class(self, class_id: str) -> None:
        """Delete class"""
        self.classes.delete_one({"class_id": class_id})
    
    def get_all_classes(self, teacher_id: str) -> List[Dict[str, Any]]:
        """Get all classes for a teacher"""
        classes = list(self.classes.find({"teacher_id": teacher_id}, {"_id": 0}))
        return classes
    
    def create_student(self, student_id: str, student_data: Dict[str, Any]) -> None:
        """Create a new student"""
        student_data['student_id'] = student_id
        student_data['created_at'] = datetime.utcnow().isoformat()
        self.students.insert_one(student_data)
    
    def get_student(self, student_id: str) -> Optional[Dict[str, Any]]:
        """Get student by ID"""
        student = self.students.find_one({"student_id": student_id}, {"_id": 0})
        return student
    
    def update_student(self, student_id: str, student_data: Dict[str, Any]) -> None:
        """Update student data"""
        student_data['updated_at'] = datetime.utcnow().isoformat()
        self.students.update_one(
            {"student_id": student_id},
            {"$set": student_data}
        )
    
    def create_enrollment(self, enrollment_data: Dict[str, Any]) -> None:
        """Create enrollment record"""
        enrollment_data['created_at'] = datetime.utcnow().isoformat()
        self.enrollments.insert_one(enrollment_data)
    
    def get_enrollments(self, class_id: str) -> List[Dict[str, Any]]:
        """Get all enrollments for a class"""
        enrollments = list(self.enrollments.find({"class_id": class_id}, {"_id": 0}))
        return enrollments
    
    def update_enrollment(self, class_id: str, student_id: str, enrollment_data: Dict[str, Any]) -> None:
        """Update enrollment status"""
        enrollment_data['updated_at'] = datetime.utcnow().isoformat()
        self.enrollments.update_one(
            {"class_id": class_id, "student_id": student_id},
            {"$set": enrollment_data}
        )
    
    def save_contact_message(self, message_data: Dict[str, Any]) -> None:
        """Save contact form message"""
        message_data['created_at'] = datetime.utcnow().isoformat()
        self.contact_messages.insert_one(message_data)
    
    def get_contact_messages(self, email: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get contact messages"""
        query = {"email": email} if email else {}
        messages = list(self.contact_messages.find(query, {"_id": 0}))
        return messages
    
    def create_qr_session(self, class_id: str, date: str, session_data: Dict[str, Any]) -> None:
        """Create QR session"""
        session_data['class_id'] = class_id
        session_data['date'] = date
        self.qr_sessions.insert_one(session_data)
    
    def get_qr_session(self, class_id: str, date: str) -> Optional[Dict[str, Any]]:
        """Get QR session"""
        session = self.qr_sessions.find_one(
            {"class_id": class_id, "date": date},
            {"_id": 0}
        )
        return session
    
    def update_qr_session(self, class_id: str, date: str, session_data: Dict[str, Any]) -> None:
        """Update QR session"""
        session_data['updated_at'] = datetime.utcnow().isoformat()
        self.qr_sessions.update_one(
            {"class_id": class_id, "date": date},
            {"$set": session_data},
            upsert=True
        )


class FileBackend(DatabaseBackend):
    """File-based storage implementation (original behavior)"""
    
    def __init__(self, base_dir: str = "data"):
        self.base_dir = base_dir
        self.users_dir = os.path.join(base_dir, "users")
        self.students_dir = os.path.join(base_dir, "students")
        self.contact_dir = os.path.join(base_dir, "contact")
        self.enrollments_dir = os.path.join(base_dir, "enrollments")
        self.qr_sessions_dir = os.path.join(base_dir, "qr_sessions")
        self._ensure_directories()
    
    def _ensure_directories(self):
        """Ensure all base directories exist"""
        os.makedirs(self.users_dir, exist_ok=True)
        os.makedirs(self.students_dir, exist_ok=True)
        os.makedirs(self.contact_dir, exist_ok=True)
        os.makedirs(self.enrollments_dir, exist_ok=True)
        os.makedirs(self.qr_sessions_dir, exist_ok=True)
    
    def _read_json(self, file_path: str) -> Optional[Dict[Any, Any]]:
        """Read JSON file safely"""
        try:
            if not os.path.exists(file_path):
                return None
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error reading {file_path}: {e}")
            return None
    
    def _write_json(self, file_path: str, data: Dict[Any, Any]):
        """Write JSON file safely"""
        try:
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"Error writing {file_path}: {e}")
            raise
    
    def create_user(self, user_id: str, user_data: Dict[str, Any]) -> None:
        """Create a new user"""
        user_dir = os.path.join(self.users_dir, user_id)
        os.makedirs(user_dir, exist_ok=True)
        user_file = os.path.join(user_dir, "user.json")
        user_data['created_at'] = datetime.utcnow().isoformat()
        self._write_json(user_file, user_data)
    
    def get_user(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Get user by ID"""
        user_file = os.path.join(self.users_dir, user_id, "user.json")
        return self._read_json(user_file)
    
    def update_user(self, user_id: str, user_data: Dict[str, Any]) -> None:
        """Update user data"""
        user_file = os.path.join(self.users_dir, user_id, "user.json")
        user_data['updated_at'] = datetime.utcnow().isoformat()
        self._write_json(user_file, user_data)
    
    def delete_user(self, user_id: str) -> None:
        """Delete user"""
        user_dir = os.path.join(self.users_dir, user_id)
        if os.path.exists(user_dir):
            shutil.rmtree(user_dir)
    
    def get_user_by_email(self, email: str) -> Optional[Dict[str, Any]]:
        """Get user by email"""
        if not os.path.exists(self.users_dir):
            return None
        
        for user_id in os.listdir(self.users_dir):
            user = self.get_user(user_id)
            if user and user.get('email') == email:
                return user
        return None
    
    def create_class(self, teacher_id: str, class_id: str, class_data: Dict[str, Any]) -> None:
        """Create a new class"""
        classes_dir = os.path.join(self.users_dir, teacher_id, "classes")
        os.makedirs(classes_dir, exist_ok=True)
        class_file = os.path.join(classes_dir, f"class_{class_id}.json")
        class_data['created_at'] = datetime.utcnow().isoformat()
        self._write_json(class_file, class_data)
    
    def get_class(self, class_id: str) -> Optional[Dict[str, Any]]:
        """Get class by ID - searches across all teachers"""
        if not os.path.exists(self.users_dir):
            return None
        
        for teacher_id in os.listdir(self.users_dir):
            class_file = os.path.join(self.users_dir, teacher_id, "classes", f"class_{class_id}.json")
            class_data = self._read_json(class_file)
            if class_data:
                return class_data
        return None
    
    def update_class(self, class_id: str, class_data: Dict[str, Any]) -> None:
        """Update class data"""
        # Find the teacher for this class
        teacher_id = class_data.get('teacher_id')
        if not teacher_id:
            # Search for it
            existing = self.get_class(class_id)
            if existing:
                teacher_id = existing.get('teacher_id')
        
        if teacher_id:
            class_file = os.path.join(self.users_dir, teacher_id, "classes", f"class_{class_id}.json")
            class_data['updated_at'] = datetime.utcnow().isoformat()
            self._write_json(class_file, class_data)
    
    def delete_class(self, class_id: str) -> None:
        """Delete class"""
        # Find and delete the class file
        if not os.path.exists(self.users_dir):
            return
        
        for teacher_id in os.listdir(self.users_dir):
            class_file = os.path.join(self.users_dir, teacher_id, "classes", f"class_{class_id}.json")
            if os.path.exists(class_file):
                os.remove(class_file)
                break
    
    def get_all_classes(self, teacher_id: str) -> List[Dict[str, Any]]:
        """Get all classes for a teacher"""
        classes_dir = os.path.join(self.users_dir, teacher_id, "classes")
        if not os.path.exists(classes_dir):
            return []
        
        classes = []
        for filename in os.listdir(classes_dir):
            if filename.startswith("class_") and filename.endswith(".json"):
                class_data = self._read_json(os.path.join(classes_dir, filename))
                if class_data:
                    classes.append(class_data)
        return classes
    
    def create_student(self, student_id: str, student_data: Dict[str, Any]) -> None:
        """Create a new student"""
        student_dir = os.path.join(self.students_dir, student_id)
        os.makedirs(student_dir, exist_ok=True)
        student_file = os.path.join(student_dir, "student.json")
        student_data['created_at'] = datetime.utcnow().isoformat()
        self._write_json(student_file, student_data)
    
    def get_student(self, student_id: str) -> Optional[Dict[str, Any]]:
        """Get student by ID"""
        student_file = os.path.join(self.students_dir, student_id, "student.json")
        return self._read_json(student_file)
    
    def update_student(self, student_id: str, student_data: Dict[str, Any]) -> None:
        """Update student data"""
        student_file = os.path.join(self.students_dir, student_id, "student.json")
        student_data['updated_at'] = datetime.utcnow().isoformat()
        self._write_json(student_file, student_data)
    
    def create_enrollment(self, enrollment_data: Dict[str, Any]) -> None:
        """Create enrollment record"""
        class_id = enrollment_data.get('class_id')
        enrollment_file = os.path.join(self.enrollments_dir, f"class_{class_id}_enrollments.json")
        
        enrollments = self._read_json(enrollment_file) or []
        enrollment_data['created_at'] = datetime.utcnow().isoformat()
        enrollments.append(enrollment_data)
        self._write_json(enrollment_file, enrollments)
    
    def get_enrollments(self, class_id: str) -> List[Dict[str, Any]]:
        """Get all enrollments for a class"""
        enrollment_file = os.path.join(self.enrollments_dir, f"class_{class_id}_enrollments.json")
        return self._read_json(enrollment_file) or []
    
    def update_enrollment(self, class_id: str, student_id: str, enrollment_data: Dict[str, Any]) -> None:
        """Update enrollment status"""
        enrollment_file = os.path.join(self.enrollments_dir, f"class_{class_id}_enrollments.json")
        enrollments = self._read_json(enrollment_file) or []
        
        for enrollment in enrollments:
            if enrollment.get('student_id') == student_id:
                enrollment.update(enrollment_data)
                enrollment['updated_at'] = datetime.utcnow().isoformat()
                break
        
        self._write_json(enrollment_file, enrollments)
    
    def save_contact_message(self, message_data: Dict[str, Any]) -> None:
        """Save contact form message"""
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        message_file = os.path.join(self.contact_dir, f"message_{timestamp}.json")
        message_data['created_at'] = datetime.utcnow().isoformat()
        self._write_json(message_file, message_data)
    
    def get_contact_messages(self, email: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get contact messages"""
        if not os.path.exists(self.contact_dir):
            return []
        
        messages = []
        for filename in os.listdir(self.contact_dir):
            if filename.endswith('.json'):
                message = self._read_json(os.path.join(self.contact_dir, filename))
                if message:
                    if email is None or message.get('email') == email:
                        messages.append(message)
        
        return sorted(messages, key=lambda x: x.get('created_at', ''), reverse=True)
    
    def create_qr_session(self, class_id: str, date: str, session_data: Dict[str, Any]) -> None:
        """Create QR session"""
        session_file = os.path.join(self.qr_sessions_dir, f"class_{class_id}_{date}.json")
        self._write_json(session_file, session_data)
    
    def get_qr_session(self, class_id: str, date: str) -> Optional[Dict[str, Any]]:
        """Get QR session"""
        session_file = os.path.join(self.qr_sessions_dir, f"class_{class_id}_{date}.json")
        return self._read_json(session_file)
    
    def update_qr_session(self, class_id: str, date: str, session_data: Dict[str, Any]) -> None:
        """Update QR session"""
        session_file = os.path.join(self.qr_sessions_dir, f"class_{class_id}_{date}.json")
        session_data['updated_at'] = datetime.utcnow().isoformat()
        self._write_json(session_file, session_data)


class DatabaseManager:
    """
    Unified Database Manager that works with multiple backends
    Automatically selects backend based on configuration
    """
    
    def __init__(self, backend_type: str = "file", **kwargs):
        """
        Initialize database manager with specified backend
        
        Args:
            backend_type: "file" or "mongodb"
            **kwargs: Backend-specific configuration
                For file: base_dir (default: "data")
                For mongodb: mongo_uri (required), database_name (default: "lernova_db")
        """
        self.backend_type = backend_type
        
        if backend_type == "mongodb":
            mongo_uri = kwargs.get('mongo_uri')
            if not mongo_uri:
                raise ValueError("mongo_uri is required for MongoDB backend")
            database_name = kwargs.get('database_name', 'lernova_db')
            self.backend = MongoDBBackend(mongo_uri, database_name)
            print(f"✅ DatabaseManager initialized with MongoDB backend")
            
        elif backend_type == "file":
            base_dir = kwargs.get('base_dir', 'data')
            self.backend = FileBackend(base_dir)
            print(f"✅ DatabaseManager initialized with File backend (dir: {base_dir})")
            
        else:
            raise ValueError(f"Unsupported backend type: {backend_type}")
    
    # Delegate all methods to the backend
    def create_user(self, user_id: str, user_data: Dict[str, Any]) -> None:
        return self.backend.create_user(user_id, user_data)
    
    def get_user(self, user_id: str) -> Optional[Dict[str, Any]]:
        return self.backend.get_user(user_id)
    
    def update_user(self, user_id: str, user_data: Dict[str, Any]) -> None:
        return self.backend.update_user(user_id, user_data)
    
    def delete_user(self, user_id: str) -> None:
        return self.backend.delete_user(user_id)
    
    def get_user_by_email(self, email: str) -> Optional[Dict[str, Any]]:
        return self.backend.get_user_by_email(email)
    
    def create_class(self, teacher_id: str, class_id: str, class_data: Dict[str, Any]) -> None:
        return self.backend.create_class(teacher_id, class_id, class_data)
    
    def get_class(self, class_id: str) -> Optional[Dict[str, Any]]:
        return self.backend.get_class(class_id)
    
    def update_class(self, class_id: str, class_data: Dict[str, Any]) -> None:
        return self.backend.update_class(class_id, class_data)
    
    def delete_class(self, class_id: str) -> None:
        return self.backend.delete_class(class_id)
    
    def get_all_classes(self, teacher_id: str) -> List[Dict[str, Any]]:
        return self.backend.get_all_classes(teacher_id)
    
    def create_student(self, student_id: str, student_data: Dict[str, Any]) -> None:
        return self.backend.create_student(student_id, student_data)
    
    def get_student(self, student_id: str) -> Optional[Dict[str, Any]]:
        return self.backend.get_student(student_id)
    
    def update_student(self, student_id: str, student_data: Dict[str, Any]) -> None:
        return self.backend.update_student(student_id, student_data)
    
    def create_enrollment(self, enrollment_data: Dict[str, Any]) -> None:
        return self.backend.create_enrollment(enrollment_data)
    
    def get_enrollments(self, class_id: str) -> List[Dict[str, Any]]:
        return self.backend.get_enrollments(class_id)
    
    def update_enrollment(self, class_id: str, student_id: str, enrollment_data: Dict[str, Any]) -> None:
        return self.backend.update_enrollment(class_id, student_id, enrollment_data)
    
    def save_contact_message(self, message_data: Dict[str, Any]) -> None:
        return self.backend.save_contact_message(message_data)
    
    def get_contact_messages(self, email: Optional[str] = None) -> List[Dict[str, Any]]:
        return self.backend.get_contact_messages(email)
    
    def create_qr_session(self, class_id: str, date: str, session_data: Dict[str, Any]) -> None:
        return self.backend.create_qr_session(class_id, date, session_data)
    
    def get_qr_session(self, class_id: str, date: str) -> Optional[Dict[str, Any]]:
        return self.backend.get_qr_session(class_id, date)
    
    def update_qr_session(self, class_id: str, date: str, session_data: Dict[str, Any]) -> None:
        return self.backend.update_qr_session(class_id, date, session_data)
    
    # ==================== COMPLEX BUSINESS LOGIC METHODS ====================
    # These methods contain the application logic and use the backend methods
    
    def _generate_qr_code(self) -> str:
        """Generate random QR code"""
        import random, string
        return ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
    
    def _count_valid_sessions_for_date(self, class_data: dict, date: str) -> int:
        """
        Count sessions with ACTUAL attendance data for a specific date
        Ignores sessions where all students have null status
        """
        print(f"[COUNT_VALID] Counting valid sessions for {date}")
        
        students = class_data.get('students', [])
        if not students:
            print(f"[COUNT_VALID] No students in class")
            return 0
        
        max_sessions = 0
        
        for student in students:
            attendance = student.get('attendance', {})
            day_data = attendance.get(date)
            
            if day_data:
                if isinstance(day_data, dict) and 'sessions' in day_data:
                    sessions = day_data.get('sessions', [])
                    valid_sessions = [s for s in sessions if s.get('status') is not None]
                    session_count = len(valid_sessions)
                    max_sessions = max(max_sessions, session_count)
                elif isinstance(day_data, dict) and 'status' in day_data:
                    if day_data.get('status') is not None:
                        count = day_data.get('count', 1)
                        max_sessions = max(max_sessions, count)
                elif isinstance(day_data, str):
                    max_sessions = max(max_sessions, 1)
        
        print(f"[COUNT_VALID] Result: {max_sessions} valid sessions")
        return max_sessions
    
    def start_qr_session(self, class_id: str, teacher_id: str, date: str, rotation_interval: int = 5) -> dict:
        """Start QR session for attendance"""
        print(f"\n{'='*60}")
        print(f"[QR_SESSION] Starting QR session for class {class_id}, date {date}")
        
        class_data = self.get_class(class_id)
        if not class_data or class_data.get("teacher_id") != teacher_id:
            raise ValueError("Class not found or unauthorized")
        
        enrollment_mode = class_data.get("enrollment_mode", "manual_entry")
        if enrollment_mode != "enrollment_via_id":
            raise ValueError("QR attendance is only available for classes with student enrollment via Class ID")
        
        valid_session_count = self._count_valid_sessions_for_date(class_data, date)
        
        existing_session = self.get_qr_session(class_id, date)
        
        if existing_session and existing_session.get("status") == "active":
            raise ValueError("There is already an active QR session for this date. Please stop it first.")
        
        session_number = valid_session_count + 1
        
        qr_session_data = {
            "class_id": class_id,
            "teacher_id": teacher_id,
            "date": date,
            "session_number": session_number,
            "started_at": datetime.utcnow().isoformat(),
            "rotation_interval": rotation_interval,
            "current_code": self._generate_qr_code(),
            "code_generated_at": datetime.utcnow().isoformat(),
            "scanned_students": [],
            "status": "active"
        }
        
        self.create_qr_session(class_id, date, qr_session_data)
        print(f"[QR_SESSION] Session #{session_number} started")
        print(f"{'='*60}\n")
        
        return qr_session_data
    
    def get_active_qr_session(self, class_id: str, date: str) -> Optional[dict]:
        """Get active QR session with auto-rotation"""
        session_data = self.get_qr_session(class_id, date)
        
        if not session_data or session_data.get("status") != "active":
            return None
        
        # Auto-rotate code
        code_time = datetime.fromisoformat(session_data["code_generated_at"])
        elapsed = (datetime.utcnow() - code_time).total_seconds()
        
        if elapsed >= session_data["rotation_interval"]:
            session_data["current_code"] = self._generate_qr_code()
            session_data["code_generated_at"] = datetime.utcnow().isoformat()
            self.update_qr_session(class_id, date, session_data)
            print(f"[QR] Auto-rotated code for {class_id} on {date}")
        
        return session_data
    
    def scan_qr_code(self, student_id: str, class_id: str, qr_code: str, date: str) -> Dict[str, Any]:
        """Handle QR code scan for attendance"""
        print(f"\n{'='*60}")
        print(f"[DB_QR_SCAN] Processing QR scan")
        print(f"  Student ID: {student_id}, Class ID: {class_id}, Date: {date}")
        print(f"{'='*60}")
        
        # Load QR session
        session_data = self.get_qr_session(class_id, date)
        
        if not session_data or session_data.get("status") != "active":
            raise ValueError("No active QR session")
        
        # Validate QR code
        try:
            qr_data = json.loads(qr_code)
            qr_code_value = qr_data.get("code")
        except:
            qr_code_value = qr_code
        
        if session_data.get("current_code") != qr_code_value:
            raise ValueError("Invalid or expired QR code")
        
        qr_session_number = session_data.get("session_number", 1)
        
        # Find enrollment
        all_enrollments = self.get_enrollments(class_id)
        
        enrollment = None
        for e in all_enrollments:
            if e.get("student_id") == student_id and e.get("status") == "active":
                enrollment = e
                break
        
        if not enrollment:
            raise ValueError("Student not actively enrolled in this class")
        
        student_record_id = enrollment.get("student_record_id")
        
        # Get class data
        class_data = self.get_class(class_id)
        if not class_data:
            raise ValueError("Class not found")
        
        teacher_id = class_data.get("teacher_id")
        
        # Find student record in class
        students = class_data.get('students', [])
        student_record = None
        for s in students:
            if s.get('id') == student_record_id:
                student_record = s
                break
        
        if not student_record:
            raise ValueError("Student record not found")
        
        # Initialize attendance
        if 'attendance' not in student_record:
            student_record['attendance'] = {}
        
        current_value = student_record['attendance'].get(date)
        
        # Update attendance based on session number
        if qr_session_number == 1:
            student_record['attendance'][date] = 'P'
            print(f"[DB_QR_SCAN] Session 1: Marked 'P'")
        else:
            # Create or update sessions array
            if isinstance(current_value, str) or current_value is None:
                sessions = []
                for i in range(1, qr_session_number + 1):
                    sessions.append({
                        "id": f"session_{i}",
                        "name": f"QR Session {i}",
                        "status": current_value if (i == 1 and isinstance(current_value, str)) else ("P" if i == qr_session_number else "A")
                    })
                student_record['attendance'][date] = {
                    "sessions": sessions,
                    "updated_at": datetime.utcnow().isoformat()
                }
            elif isinstance(current_value, dict) and 'sessions' in current_value:
                sessions = current_value.get('sessions', [])
                existing_ids = {s.get('id') for s in sessions}
                
                for i in range(1, qr_session_number + 1):
                    session_id = f"session_{i}"
                    if session_id not in existing_ids:
                        sessions.insert(i - 1, {
                            "id": session_id,
                            "name": f"QR Session {i}",
                            "status": "P" if i == qr_session_number else "A"
                        })
                    elif i == qr_session_number:
                        for session in sessions:
                            if session.get('id') == session_id:
                                session['status'] = 'P'
                                break
                
                student_record['attendance'][date] = {
                    "sessions": sessions,
                    "updated_at": datetime.utcnow().isoformat()
                }
        
        # Save class data
        self.update_class(class_id, class_data)
        
        # Record scan in QR session
        scanned = session_data.get("scanned_students", [])
        if student_record_id not in scanned:
            scanned.append(student_record_id)
        session_data["scanned_students"] = scanned
        session_data["last_scan_at"] = datetime.utcnow().isoformat()
        self.update_qr_session(class_id, date, session_data)
        
        print(f"[DB_QR_SCAN] SUCCESS - Session #{qr_session_number}")
        
        return {
            "success": True,
            "message": f"Attendance marked as Present (Session #{qr_session_number})",
            "scan_count": qr_session_number,
            "session_number": qr_session_number,
            "date": date,
        }
    
    def stop_qr_session(self, class_id: str, teacher_id: str, date: str) -> Dict[str, Any]:
        """Stop QR session and mark absent for non-scanned students"""
        session_data = self.get_qr_session(class_id, date)
        
        if not session_data or session_data.get("status") != "active":
            raise ValueError("No active session")
        
        if session_data.get("teacher_id") != teacher_id:
            raise ValueError("Unauthorized")
        
        qr_session_number = session_data.get("session_number", 1)
        scanned_ids = set(session_data.get("scanned_students", []))
        
        print(f"[QR_STOP] Stopping QR Session #{qr_session_number}")
        
        # Get active enrollments
        all_enrollments = self.get_enrollments(class_id)
        active_student_ids = {
            e.get("student_record_id")
            for e in all_enrollments
            if e.get("status") == "active"
        }
        
        # Get class data
        class_data = self.get_class(class_id)
        students = class_data.get('students', [])
        
        # Mark absent for non-scanned students
        marked_absent = 0
        for student in students:
            student_record_id = student.get('id')
            if student_record_id in active_student_ids and student_record_id not in scanned_ids:
                if 'attendance' not in student:
                    student['attendance'] = {}
                
                current_value = student['attendance'].get(date)
                
                if qr_session_number == 1:
                    student['attendance'][date] = 'A'
                else:
                    if isinstance(current_value, str) or current_value is None:
                        sessions = []
                        for i in range(1, qr_session_number + 1):
                            sessions.append({
                                "id": f"session_{i}",
                                "name": f"QR Session {i}",
                                "status": current_value if (i == 1 and isinstance(current_value, str)) else "A"
                            })
                        student['attendance'][date] = {
                            "sessions": sessions,
                            "updated_at": datetime.utcnow().isoformat()
                        }
                    elif isinstance(current_value, dict) and 'sessions' in current_value:
                        sessions = current_value.get('sessions', [])
                        existing_ids = {s.get('id') for s in sessions}
                        
                        for i in range(1, qr_session_number + 1):
                            session_id = f"session_{i}"
                            if session_id not in existing_ids:
                                sessions.insert(i - 1, {
                                    "id": session_id,
                                    "name": f"QR Session {i}",
                                    "status": "A"
                                })
                        
                        student['attendance'][date] = {
                            "sessions": sessions,
                            "updated_at": datetime.utcnow().isoformat()
                        }
                
                marked_absent += 1
        
        # Save class data
        self.update_class(class_id, class_data)
        
        # Close QR session
        session_data["status"] = "stopped"
        session_data["stopped_at"] = datetime.utcnow().isoformat()
        self.update_qr_session(class_id, date, session_data)
        
        print(f"[QR_STOP] Session {qr_session_number} stopped")
        
        return {
            "success": True,
            "scanned_count": len(scanned_ids),
            "absent_count": marked_absent,
            "date": date,
            "session_number": qr_session_number
        }